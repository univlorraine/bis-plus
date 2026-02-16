"""Task de vérification post-rollback."""
import logging
from typing import Dict

from airflow.sdk import task
from airflow.exceptions import AirflowException

from amue.services.bluegreen.rollback_manager import RollbackManager

logger = logging.getLogger(__name__)


@task(task_id='verify_rollback')
def verify_rollback(rollback_result: Dict) -> Dict:
    """
    Vérifie que le rollback a réussi.

    Contrôle que les vues pointent vers le bon schéma.

    Args:
        rollback_result: Résultat de execute_rollback()

    Returns:
        Résultat de la vérification

    Raises:
        AirflowException: Si la vérification échoue
    """
    manager = RollbackManager()
    verification = manager.verify_rollback_integrity()

    if not verification.get('verified'):
        logger.error("[ROLLBACK] Vérification échouée!")
        logger.error(f"[ROLLBACK] Schéma attendu: {verification.get('expected_schema')}")
        logger.error(f"[ROLLBACK] Schéma actuel: {verification.get('actual_schema')}")
        raise AirflowException("Vérification du rollback échouée")

    logger.info("[ROLLBACK] === Vérification OK ===")
    logger.info(f"[ROLLBACK] Schéma actif: {verification.get('expected_schema')}")
    logger.info("[ROLLBACK] Toutes les vues pointent vers le bon schéma")

    return {
        'status': 'success',
        'new_active_schema': verification.get('expected_schema'),
        'rollback_from': rollback_result.get('previous_schema'),
        'rollback_to': rollback_result.get('new_schema')
    }
