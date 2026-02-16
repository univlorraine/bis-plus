"""Task de prévisualisation du rollback."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.rollback_manager import RollbackManager

logger = logging.getLogger(__name__)


@task(task_id='preview_rollback')
def preview_rollback(check_result: Dict) -> Dict:
    """
    Prévisualise le rollback avant exécution.

    Args:
        check_result: Résultat de check_rollback_available()

    Returns:
        Prévisualisation du rollback
    """
    manager = RollbackManager()
    preview = manager.preview_rollback()

    logger.info("[ROLLBACK] === Prévisualisation ===")
    logger.info(f"[ROLLBACK] De: {preview.get('from_schema')}")
    logger.info(f"[ROLLBACK] Vers: {preview.get('to_schema')}")
    logger.info(f"[ROLLBACK] Dernier switch: {preview.get('last_switch')}")

    return preview
