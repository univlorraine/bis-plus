"""Task d'envoi du rapport de rafraîchissement des vues custom."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.notifications.notifier import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='send_refresh_report', multiple_outputs=False)
def send_refresh_report(refresh_result: Dict) -> Dict:
    """
    Envoie un rapport du rafraîchissement des vues custom par email.

    Args:
        refresh_result: Résultat de refresh_custom_views()

    Returns:
        {"sent": bool, "ok": int, "ko": int}
    """
    ok = refresh_result.get('ok', 0)
    ko = refresh_result.get('ko', 0)
    target = refresh_result.get('target_schema', '?')
    files = refresh_result.get('files_processed', [])

    logger.info(f"[REFRESH_VIEWS] Rapport : {ok} vues OK, {ko} en échec → {target}")

    try:
        service = NotificationService()
        service.notify_refresh_views_success({
            'dag_id': 'amue_refresh_views',
            'target_schema': target,
            'ok': ok,
            'ko': ko,
            'files_processed': files,
            'files_failed': refresh_result.get('files_failed', []),
        })
        return {"sent": True, "ok": ok, "ko": ko}
    except Exception as e:
        logger.warning(f"[REFRESH_VIEWS] Envoi rapport échoué (non bloquant) : {e}")
        return {"sent": False, "error": str(e)}
