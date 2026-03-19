# ecc/notifications/ecc_callbacks.py
"""Callback Airflow pour les notifications d'echec ECC."""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def send_ecc_failure_notification(context: Dict[str, Any]) -> None:
    """
    Callback Airflow pour envoyer une notification en cas d'echec du DAG ECC.

    Usage dans le DAG :
        @dag(on_failure_callback=send_ecc_failure_notification, ...)

    Args:
        context: Contexte Airflow (task_instance, exception, dag_run, execution_date)
    """
    logger.info("[ECC] Declenchement du callback d'erreur")

    exception = context.get('exception')
    if not exception:
        logger.warning(
            "[ECC] Callback DAG sans exception dans le contexte — "
            "envoi d'une notification generique d'echec"
        )
        dag_run = context.get('dag_run')
        if dag_run:
            context.setdefault('error_message',
                "Le DAG ECC a echoue - consulter les logs des taches en echec pour le detail")
            context.setdefault('error_type', 'DAGFailure')

    try:
        from ecc.notifications.ecc_notifier import ECCNotificationService

        service = ECCNotificationService()
        success = service.notify_error(context)

        if success:
            logger.info("[ECC] Notification d'erreur envoyee avec succes")
        else:
            logger.warning("[ECC] Echec de l'envoi de la notification d'erreur")

    except Exception as e:
        logger.error(f"[ECC] Erreur dans le callback de notification: {e}")

    logger.info("[ECC] Callback d'erreur termine")
