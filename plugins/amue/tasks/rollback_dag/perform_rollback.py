"""Task d'exécution du rollback blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
from amue.services.bluegreen.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


@task(task_id='perform_rollback')
def perform_rollback(check_result: Dict) -> Dict:
    """
    Effectue le rollback blue/green en trois étapes atomiques :
      1. Restaure le schéma offline (splus_X_offline → splus_X)
      2. Switche les vues vers le schéma restauré
      3. Met l'ancien schéma actif en offline

    Args:
        check_result: Résultat de check_rollback()

    Returns:
        Dict avec previous_active, new_active, switched

    Raises:
        RuntimeError: Si le switch de vues échoue
    """
    manager = BlueGreenManager()
    switcher = ViewSwitcher()

    active = check_result['active_schema']          # ex: 'splus_green'
    rollback_target = check_result['rollback_target']  # ex: 'splus_blue'

    logger.info(f"[ROLLBACK] Démarrage rollback : {active} → {rollback_target}")

    # 1. Restore le schéma offline (splus_blue_offline → splus_blue)
    manager.rename_schema_from_offline(rollback_target)
    logger.info(f"[ROLLBACK] Schéma {rollback_target}_offline restauré")

    # 2. Switch les vues vers le schéma restauré
    success = switcher.switch_views_to_schema(rollback_target)
    if not success:
        # Rollback partiel : remettre le schéma en offline pour cohérence
        manager.rename_schema_to_offline(rollback_target)
        raise RuntimeError(
            f"Rollback échoué : impossible de switcher les vues vers {rollback_target}"
        )

    # 3. Mettre l'ancien actif en offline
    manager.rename_schema_to_offline(active)
    logger.info(f"[ROLLBACK] Ancien schéma {active} mis en offline")

    # 4. Mettre à jour l'état
    manager.mark_switch_completed()

    logger.info(f"[ROLLBACK] Rollback réussi : {active} → {rollback_target}")
    return {
        "rolled_back": True,
        "previous_active": active,
        "new_active": rollback_target,
    }
