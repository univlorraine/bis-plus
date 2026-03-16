"""Task d'envoi du rapport de rollback."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.notifications.notifier import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='send_rollback_report')
def send_rollback_report(rollback_result: Dict) -> Dict:
    """
    Envoie un rapport de rollback par email.

    Args:
        rollback_result: Résultat de perform_rollback()

    Returns:
        Statut de l'envoi
    """
    previous = rollback_result.get('previous_active', '?')
    new_active = rollback_result.get('new_active', '?')

    logger.info(
        f"[ROLLBACK] Rapport : rollback effectué {previous} → {new_active}"
    )

    try:
        service = NotificationService()
        service.notify_success({
            'dag_id': 'amue_rollback',
            'tables_imported': [],
            'extra_message': (
                f"Rollback blue/green effectué avec succès.\n"
                f"Schéma précédent : {previous}\n"
                f"Schéma restauré  : {new_active}"
            ),
        })
        return {"sent": True, "previous_active": previous, "new_active": new_active}
    except Exception as e:
        logger.warning(f"[ROLLBACK] Envoi rapport échoué (non bloquant): {e}")
        return {"sent": False, "error": str(e)}
