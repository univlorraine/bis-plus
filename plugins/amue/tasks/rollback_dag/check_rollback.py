"""Task de vérification de la faisabilité du rollback blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


@task(task_id='check_rollback')
def check_rollback() -> Dict:
    """
    Vérifie que le rollback est possible.

    Un rollback est possible si le schéma inactif existe en version _offline
    (créé lors du dernier switch de vues).

    Returns:
        Dict avec rollback_target, active_schema, offline_schema

    Raises:
        RuntimeError: Si aucun schéma offline n'est disponible
    """
    manager = BlueGreenManager()
    active = manager.get_active_schema()       # ex: 'splus_green'
    inactive = manager.get_inactive_schema()   # ex: 'splus_blue'
    offline = f"{inactive}_offline"            # ex: 'splus_blue_offline'

    if not manager.schema_exists(offline):
        raise RuntimeError(
            f"Rollback impossible : schéma offline introuvable ({offline}). "
            f"Le rollback n'est disponible qu'après un premier import réussi."
        )

    logger.info(
        f"[ROLLBACK] Rollback possible : {active} → {inactive} "
        f"(via {offline})"
    )
    return {
        "rollback_possible": True,
        "active_schema": active,
        "rollback_target": inactive,
        "offline_schema": offline,
    }
