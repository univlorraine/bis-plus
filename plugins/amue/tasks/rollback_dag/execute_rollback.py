"""Task d'exécution du rollback."""
import logging
from typing import Dict

from airflow.sdk import task
from airflow.exceptions import AirflowException

from amue.services.bluegreen.rollback_manager import RollbackManager

logger = logging.getLogger(__name__)


@task(task_id='execute_rollback')
def execute_rollback(preview_result: Dict) -> Dict:
    """
    Exécute le rollback.

    Switch les vues vers le schéma de rollback.

    Args:
        preview_result: Résultat de preview_rollback()

    Returns:
        Résultat du rollback

    Raises:
        AirflowException: Si le rollback échoue
    """
    manager = RollbackManager()
    result = manager.rollback()

    if not result.get('success'):
        error = result.get('error', 'Erreur inconnue')
        logger.error(f"[ROLLBACK] Échec: {error}")
        raise AirflowException(f"Rollback échoué: {error}")

    logger.info("[ROLLBACK] === Rollback effectué ===")
    logger.info(f"[ROLLBACK] Ancien schéma: {result.get('previous_schema')}")
    logger.info(f"[ROLLBACK] Nouveau schéma: {result.get('new_schema')}")

    return result
