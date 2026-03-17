"""Task d'exécution de la synchronisation entre schémas blue/green."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.services.bluegreen.schema_synchronizer import SchemaSynchronizer
from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


@task(task_id='run_sync')
def run_sync(sync_ctx: Dict) -> Dict:
    """
    Copie toutes les tables du schéma actif vers le schéma inactif.

    Utilise SchemaSynchronizer (TRUNCATE + INSERT par table avec commit
    intermédiaire). En cas de succès, enregistre last_sync_timestamp en BDD.

    Args:
        sync_ctx: Contexte retourné par init_sync()

    Returns:
        Dict résultat avec status, tables_synced, tables_failed, total_rows_copied
    """
    if not sync_ctx.get('enabled'):
        logger.info("[SYNC] Blue/green désactivé - synchronisation ignorée")
        return {
            'status': 'skipped',
            'reason': 'bluegreen_disabled',
            'tables_synced': 0,
            'tables_failed': 0,
            'total_rows_copied': 0,
            'details': [],
        }

    source = sync_ctx['source_schema']
    target = sync_ctx['target_schema']

    logger.info(f"[SYNC] Démarrage synchronisation: {source} → {target}")

    synchronizer = SchemaSynchronizer()
    result = synchronizer.sync_schemas(source, target)

    if result.get('status') in ('success', 'partial'):
        BlueGreenManager().mark_sync_completed()
        BlueGreenManager().rename_schema_to_offline(target)
        logger.info(
            f"[SYNC] Terminée: {result.get('tables_synced', 0)} tables, "
            f"{result.get('total_rows_copied', 0):,} lignes"
        )
    else:
        logger.error(
            f"[SYNC] Échec — schéma {target!r} non renommé en offline pour "
            f"permettre un retry propre: {result}"
        )

    return result
