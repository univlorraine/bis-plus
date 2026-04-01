# ecc/notifications/ecc_callbacks.py
"""Callback Airflow pour les notifications d'echec ECC."""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def send_ecc_failure_notification(context: Dict[str, Any]) -> None:
    """
    Callback Airflow pour envoyer une notification en cas d'échec du DAG ECC.

    Usage dans le DAG :
        @dag(on_failure_callback=send_ecc_failure_notification, ...)

    Args:
        context: Contexte Airflow (task_instance, exception, dag_run, execution_date)
    """
    logger.info("[ECC] Déclenchement du callback d'erreur")

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
                    "Le DAG ECC a échoué — consulter les logs des tâches pour le détail")
            context.setdefault('error_type', 'DAGFailure')

    try:
        from ecc.notifications.ecc_notifier import ECCNotificationService

        service = ECCNotificationService()
        success = service.notify_error(context)

        if success:
            logger.info("[ECC] Notification d'erreur envoyée avec succès")
        else:
            logger.warning("[ECC] Échec de l'envoi de la notification d'erreur")

    except Exception as e:
        logger.error(f"[ECC] Erreur dans le callback de notification: {e}")

    logger.info("[ECC] Callback d'erreur terminé")
