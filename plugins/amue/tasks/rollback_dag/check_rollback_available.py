"""Task de vérification de la disponibilité du rollback."""
import logging
from typing import Dict

from airflow.sdk import task
from airflow.exceptions import AirflowException

from amue.services.bluegreen.rollback_manager import RollbackManager

logger = logging.getLogger(__name__)


@task(task_id='check_rollback_available')
def check_rollback_available() -> Dict:
    """
    Vérifie que le rollback est possible.

    Conditions vérifiées :
        - Mode blue/green activé
        - Pas d'import en cours
        - Rollback disponible (pas encore sync)

    Returns:
        Informations sur le rollback disponible

    Raises:
        AirflowException: Si rollback impossible
    """
    manager = RollbackManager()
    info = manager.get_rollback_info()

    if not info.get('available'):
        reason = info.get('reason', 'Raison inconnue')
        logger.error(f"[ROLLBACK] Non disponible: {reason}")
        raise AirflowException(f"Rollback impossible: {reason}")

    logger.info(f"[ROLLBACK] Disponible")
    logger.info(f"[ROLLBACK] Schéma actuel: {info.get('current_schema')}")
    logger.info(f"[ROLLBACK] Schéma de rollback: {info.get('rollback_schema')}")
    logger.info(f"[ROLLBACK] Dernier switch: {info.get('last_switch')}")

    return info
