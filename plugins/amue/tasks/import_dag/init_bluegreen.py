"""Task d'initialisation du contexte blue/green."""
import logging
from typing import Dict

from airflow.sdk import task, get_current_context

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


@task(task_id='init_bluegreen')
def init_bluegreen() -> Dict:
    """
    Initialise le contexte blue/green pour ce DAG run.

    Détermine le schéma cible (opposé de l'actif) et prépare l'état.

    Returns:
        Contexte blue/green :
        {
            "enabled": True/False,
            "target_schema": "splus_green",
            "active_schema": "splus_blue",
            "needs_sync": True/False
        }
    """
    manager = BlueGreenManager()

    if not manager.is_enabled():
        logger.info("[BLUEGREEN] Mode désactivé - import classique")
        return {
            "enabled": False,
            "target_schema": None,
            "active_schema": None,
            "needs_sync": False
        }

    target = manager.get_target_schema()
    manager.rename_schema_from_offline(target)
    active = manager.get_active_schema()
    needs_sync = manager.needs_sync()

    logger.info(f"[BLUEGREEN] Mode activé")
    logger.info(f"[BLUEGREEN] Schéma actif: {active}")
    logger.info(f"[BLUEGREEN] Schéma cible: {target}")
    logger.info(f"[BLUEGREEN] Sync nécessaire: {needs_sync}")

    # Récupère le run_id comme correlation_id automatique
    try:
        context = get_current_context()
        run_id = context["dag_run"].run_id if context.get("dag_run") else "unknown"
    except Exception:
        run_id = "unknown"

    # Marque le début de l'import avec correlation_id
    manager.mark_import_started(correlation_id=run_id)

    return {
        "enabled": True,
        "target_schema": target,
        "active_schema": active,
        "needs_sync": needs_sync
    }
