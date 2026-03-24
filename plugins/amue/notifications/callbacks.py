# amue/notifications/callbacks.py
"""
Callbacks Airflow pour les notifications.

Ce module fournit le point d'entree unique pour les callbacks Airflow,
remplacant les implementations dupliquees dans notification_service.py
et error_notifier.py.
"""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def send_failure_notification(context: Dict[str, Any]) -> None:
    """
    Callback Airflow pour envoyer une notification en cas d'echec.

    Cette fonction est appelee automatiquement par Airflow quand:
    - Une tache echoue (task-level callback via on_failure_callback)
    - Un DAG echoue (dag-level callback via on_failure_callback)

    Usage dans le DAG:
        @dag(
            on_failure_callback=send_failure_notification,
            default_args={
                'on_failure_callback': send_failure_notification
            }
        )

    Args:
        context: Contexte Airflow contenant:
            - task_instance: Instance de la tache en echec
            - exception: Exception survenue
            - dag_run: Informations sur l'execution du DAG
            - execution_date: Date d'execution
    """
    logger.info("Declenchement du callback d'erreur")

    # Au niveau DAG, exception et task_instance sont absents du contexte.
    # On enrichit le contexte avec la liste des tâches en échec.
    exception = context.get('exception')
    if not exception:
        dag_run = context.get('dag_run')
        if dag_run:
            try:
                failed_tis = dag_run.get_task_instances(state='failed')
            except Exception:
                failed_tis = []
            if failed_tis:
                failed_names = [
                    f"{ti.task_id}[{ti.map_index}]" if getattr(ti, 'map_index', -1) >= 0 else ti.task_id
                    for ti in failed_tis
                ]
                context.setdefault('error_message',
                    f"Tâches en échec : {', '.join(failed_names)}")
                context['failed_tasks'] = [
                    {
                        'task_id': ti.task_id,
                        'map_index': getattr(ti, 'map_index', -1),
                        'duration': round(ti.duration, 1) if getattr(ti, 'duration', None) else None,
                    }
                    for ti in failed_tis
                ]
            else:
                context.setdefault('error_message',
                    "Le DAG a échoué — consulter les logs des tâches pour le détail")
            context.setdefault('error_type', 'DAGFailure')

    try:
        # Import local pour eviter les imports circulaires
        from amue.notifications.notifier import NotificationService

        service = NotificationService()
        success = service.notify_error(context)

        if success:
            logger.info("Notification d'erreur envoyee avec succes")
        else:
            logger.warning("Echec de l'envoi de la notification d'erreur")

    except Exception as e:
        logger.error(f"Erreur dans le callback de notification: {e}")

    # Tente de générer un rapport partiel si des résultats d'import existent.
    # En Airflow 3 (Task SDK), map_indexes='all' n'est plus valide.
    # On passe par le dag_run pour lister les TIs import_data réussies
    # et on tire leur XCom individuellement avec un map_index entier.
    try:
        task_instance = context.get('task_instance')
        dag_run = context.get('dag_run')
        if task_instance and dag_run:
            success_tis = dag_run.get_task_instances(state='success')
            import_success_tis = [ti for ti in success_tis if ti.task_id == 'import_data']
            import_results = []
            for ti in import_success_tis:
                result = task_instance.xcom_pull(
                    task_ids='import_data',
                    key='return_value',
                    map_index=ti.map_index,
                )
                if result is not None:
                    import_results.append(result)
            if import_results:
                from amue.notifications.report_generator import AMUEReportGenerator
                generator = AMUEReportGenerator()
                report = generator.generate_report(import_results, {})
                report['status'] = 'partial_failure'
                logger.info(f"Rapport partiel généré ({len(import_results)} table(s) traitée(s) avant échec)")
    except Exception as report_err:
        logger.warning(f"Impossible de générer le rapport partiel: {report_err}")

    logger.info("Callback d'erreur termine")


def dag_failure_rollback(context: Dict[str, Any]) -> None:
    """
    Callback Airflow niveau DAG pour le rollback blue/green après échec.

    Cette fonction est appelée au niveau du DAG (on_failure_callback sur @dag).
    Elle gère UNIQUEMENT le rollback de l'état blue/green et le déclenchement
    de amue_sync_schemas. Elle n'envoie PAS d'email (les emails sont envoyés
    au niveau des tâches via send_failure_notification dans default_args).

    Args:
        context: Contexte Airflow du DAG en échec
    """
    logger.info("Rollback blue/green après échec du DAG")

    # Rollback blue/green : libère le verrou si actif, puis remet le schéma cible en _offline.
    # Le rename est tenté indépendamment du verrou (idempotent : no-op si le schéma n'existe pas).
    # Cela couvre aussi le cas où init_bluegreen a renommé le schéma mais échoué avant mark_import_started.
    try:
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        target_schema = manager.get_target_schema()

        if manager.is_import_in_progress():
            manager.release_import_lock(mark_completed=False)
            logger.info("Verrou blue/green libéré après échec du DAG")

        renamed = manager.rename_schema_to_offline(target_schema)
        if renamed:
            logger.info(f"[ROLLBACK] Schéma {target_schema!r} → {target_schema}_offline")
        else:
            logger.info(f"[ROLLBACK] Schéma {target_schema!r} introuvable ou déjà offline — aucune action")
    except Exception as e:
        logger.error(f"Erreur lors du rollback blue/green: {e}")

    # Déclenche amue_sync_schemas pour nettoyer le schéma inactif
    try:
        from airflow.api.common.trigger_dag import trigger_dag
        from airflow.models.dagrun import DagRunTriggeredByType
        from datetime import datetime
        run_id = f"failure_cleanup_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        trigger_dag(dag_id='amue_sync_schemas', run_id=run_id, triggered_by=DagRunTriggeredByType.OPERATOR)
        logger.info(f"[ROLLBACK] amue_sync_schemas déclenché (run_id={run_id})")
    except Exception as e:
        logger.warning(f"[ROLLBACK] Impossible de déclencher amue_sync_schemas: {e}")

    logger.info("Rollback blue/green terminé")


def send_success_notification(context: Dict[str, Any]) -> None:
    """
    Callback Airflow pour envoyer une notification en cas de succes.

    Cette fonction peut etre utilisee comme callback de succes au niveau
    du DAG (on_success_callback).

    Note: Pour les imports AMUE, il est recommande d'utiliser directement
    le AMUEReportGenerator dans la task send_report plutot que ce callback,
    car il permet de generer un rapport plus detaille.

    Args:
        context: Contexte Airflow contenant:
            - task_instance: Instance de la tache
            - dag_run: Informations sur l'execution du DAG
            - execution_date: Date d'execution
    """
    logger.info("Declenchement du callback de succes")

    try:
        # Import local pour eviter les imports circulaires
        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        # Extrait les donnees du contexte Airflow
        task_instance = context.get('task_instance')
        dag_run = context.get('dag_run')

        data = {
            'dag_id': task_instance.dag_id if task_instance else 'unknown',
            'execution_date': str(context.get('execution_date', '')),
            'duration': 'N/A',
            'tables_imported': [],
        }

        # Tente de recuperer les resultats depuis XCom si disponibles
        if dag_run:
            try:
                # Recupere les resultats d'import si disponibles
                import_results = task_instance.xcom_pull(
                    task_ids='import_data',
                    key='return_value'
                )
                if import_results:
                    data['tables_imported'] = import_results
            except Exception as xcom_err:
                logger.warning(f"Impossible de récupérer les résultats XCom: {xcom_err}")

        success = service.notify_success(data)

        if success:
            logger.info("Notification de succes envoyee")
        else:
            logger.warning("Echec de l'envoi de la notification de succes")

    except Exception as e:
        logger.error(f"Erreur dans le callback de succes: {e}")

    logger.info("Callback de succes termine")
