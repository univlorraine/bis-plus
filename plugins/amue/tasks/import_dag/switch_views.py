"""Task de bascule atomique des vues blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
from amue.services.bluegreen.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


@task(task_id='switch_views')
def switch_views(metadata_result: Dict) -> Dict:
    """
    Bascule les vues vers le nouveau schéma après un import réussi.

    Cette opération est atomique : toutes les vues sont switchées
    dans une seule transaction. Une vérification post-switch confirme
    que toutes les vues pointent vers le bon schéma.

    Args:
        metadata_result: Résultat de save_metadata() avec target_schema

    Returns:
        Résultat du switch : {"switched": True/False, "verified": True/False, ...}
    """
    target_schema = metadata_result.get('target_schema')

    if not target_schema:
        logger.info("[SWITCH] Pas de schéma cible - blue/green désactivé")
        return {"switched": False, "reason": "bluegreen disabled"}

    manager = BlueGreenManager()
    if not manager.is_enabled():
        logger.info("[SWITCH] Blue/green désactivé dans la config")
        return {"switched": False, "reason": "bluegreen disabled in config"}

    switcher = ViewSwitcher()
    success = switcher.switch_views_to_schema(target_schema)

    if success:
        # Vérification post-switch
        verified = switcher.verify_views_point_to(target_schema)
        if not verified:
            logger.error(f"[SWITCH] Vérification post-switch échouée pour {target_schema}")
            return {
                "switched": False,
                "verified": False,
                "target_schema": target_schema,
                "error": "Post-switch verification failed"
            }

        manager.mark_import_completed()
        manager.mark_switch_completed()
        logger.info(f"[SWITCH] Vues basculées et vérifiées vers {target_schema}")
        return {
            "switched": True,
            "verified": True,
            "target_schema": target_schema,
            "error": None
        }
    else:
        logger.error(f"[SWITCH] Échec du switch vers {target_schema}")
        return {
            "switched": False,
            "target_schema": target_schema,
            "error": "Switch failed"
        }
