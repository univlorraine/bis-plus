# ecc/notifications/ecc_callbacks.py
"""Layer: infrastructure

Callback Airflow pour les notifications d'echec ECC."""
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

    from common.infrastructure.notifications.failure_callback_helpers import enrich_context_with_failed_tasks, notify_error_safe
    enrich_context_with_failed_tasks(context, dag_label='ECC')

    def _make_service():
        from ecc.infrastructure.notifications.ecc_notifier import ECCNotificationService
        return ECCNotificationService()

    notify_error_safe(context, _make_service, dag_label='ECC')

    logger.info("[ECC] Callback d'erreur terminé")
