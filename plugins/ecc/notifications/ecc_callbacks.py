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

    from common.notifications.callbacks_utils import enrich_context_with_failed_tasks
    enrich_context_with_failed_tasks(context, dag_label='ECC')

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
