"""Task de détection du schéma actif blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from common.services.bluegreen.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


@task(task_id='detect_active_schema', multiple_outputs=False)
def detect_active_schema() -> Dict:
    """
    Détermine le schéma actuellement actif (splus_blue ou splus_green).

    Returns:
        {"active_schema": "splus_blue"}  (ou splus_green)

    Raises:
        RuntimeError: Si aucune vue n'existe encore dans le schéma splus
    """
    switcher = ViewSwitcher()
    active = switcher.get_current_target_schema()

    if active is None:
        raise RuntimeError(
            "Impossible de déterminer le schéma actif : aucune vue trouvée "
            "dans le schéma 'splus'. Lancez un import complet d'abord."
        )

    logger.info(f"[REFRESH_VIEWS] Schéma actif détecté : {active}")
    return {"active_schema": active}
