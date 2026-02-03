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

    # Verifie qu'il y a bien une exception avant d'envoyer
    exception = context.get('exception')
    if not exception:
        logger.info("Pas d'exception dans le contexte - notification ignoree")
        return

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

    logger.info("Callback d'erreur termine")


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
            except Exception:
                pass

        success = service.notify_success(data)

        if success:
            logger.info("Notification de succes envoyee")
        else:
            logger.warning("Echec de l'envoi de la notification de succes")

    except Exception as e:
        logger.error(f"Erreur dans le callback de succes: {e}")

    logger.info("Callback de succes termine")
