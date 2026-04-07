"""Task de recréation des vues custom."""
import logging
from typing import Dict

from airflow.sdk import task

from common.services.bluegreen.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


@task(task_id='refresh_custom_views', multiple_outputs=False)
def refresh_custom_views(schema_info: Dict) -> Dict:
    """
    Recrée toutes les vues custom pointant vers le schéma actif.

    Args:
        schema_info: Résultat de detect_active_schema()

    Returns:
        {"ok": int, "ko": int, "target_schema": str, "files_processed": List[str]}
    """
    active_schema = schema_info['active_schema']
    logger.info(f"[REFRESH_VIEWS] Recréation des vues custom → {active_schema}")

    switcher = ViewSwitcher()
    result = switcher.refresh_custom_views(active_schema)

    logger.info(
        f"[REFRESH_VIEWS] Terminé : {result['ok']} OK, {result['ko']} en échec "
        f"(schéma cible : {active_schema})"
    )
    return result
