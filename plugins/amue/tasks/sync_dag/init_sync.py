"""Task d'initialisation du DAG de synchronisation blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


@task(task_id='init_sync', multiple_outputs=False)
def init_sync() -> Dict:
    """
    Prépare le contexte de synchronisation blue/green.

    Returns:
        Dict avec :
            - enabled (bool): Toujours True
            - source_schema (str): Schéma actif à copier
            - target_schema (str): Schéma cible (sera écrasé)
    """
    manager = BlueGreenManager()
    source = manager.get_active_schema()
    target = manager.get_inactive_schema()
    manager.rename_schema_from_offline(target)

    logger.info(f"[SYNC] Source: {source} → Cible: {target}")
    return {'enabled': True, 'source_schema': source, 'target_schema': target}
