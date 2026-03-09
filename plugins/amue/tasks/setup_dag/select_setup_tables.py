"""Task de sélection des tables à initialiser pour le setup."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.services.table_config_manager import TableConfigManager
from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


@task(task_id='select_setup_tables')
def select_setup_tables(conf: Dict) -> List[Dict]:
    """
    Sélectionne les tables à initialiser pour le setup.

    En mode standalone : détermine le schéma cible via BlueGreenManager.
    Quand déclenché par la DAG principale : utilise target_schema depuis conf.

    Args:
        conf: Configuration du dag_run (peut contenir 'target_schema')

    Returns:
        Liste de dicts de configuration de tables (format amue_tables),
        chacun enrichi avec 'target_schema'.
    """
    target_schema = conf.get('target_schema') if conf else None

    if not target_schema:
        manager = BlueGreenManager()
        target_schema = manager.get_target_schema()
        logger.info(f"[SETUP] Mode standalone — schéma cible: {target_schema}")
    else:
        logger.info(f"[SETUP] Déclenché par la DAG principale — schéma cible: {target_schema}")

    tables = TableConfigManager().get_tables_config()
    enabled = [t for t in tables if t.get('enable', False)]

    for t in enabled:
        t['target_schema'] = target_schema

    logger.info(f"[SETUP] {len(enabled)} table(s) activée(s) à traiter")
    for t in enabled:
        logger.info(f"  - {t.get('name')} (setup_status={t.get('setup_status', 'pending')})")

    return enabled
