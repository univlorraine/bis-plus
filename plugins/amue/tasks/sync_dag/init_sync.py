"""Task d'initialisation du DAG de synchronisation blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@task(task_id='init_sync')
def init_sync() -> Dict:
    """
    Vérifie les conditions et prépare le contexte de synchronisation.

    Retourne un dict avec les schémas source/cible, ou indique que la
    synchronisation est désactivée (bluegreen non activé).

    Returns:
        Dict avec :
            - enabled (bool): False si bluegreen désactivé
            - source_schema (str): Schéma actif à copier
            - target_schema (str): Schéma cible (sera écrasé)
    """
    enabled = VarMgr.get('amue_bluegreen_enabled', default='false').lower() == 'true'

    if not enabled:
        logger.info("[SYNC] Mode blue/green désactivé - synchronisation ignorée")
        return {'enabled': False, 'source_schema': '', 'target_schema': ''}

    manager = BlueGreenManager()
    source = manager.get_active_schema()
    target = manager.get_inactive_schema()

    logger.info(f"[SYNC] Source: {source} → Cible: {target}")
    return {'enabled': True, 'source_schema': source, 'target_schema': target}
