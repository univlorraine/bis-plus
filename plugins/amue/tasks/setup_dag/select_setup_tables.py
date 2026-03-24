"""Task de sélection des tables à initialiser pour le setup."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.services.table_config_manager import TableConfigManager
from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


def _read_dag_run_conf() -> Dict:
    """Lit dag_run.conf depuis le contexte Airflow courant."""
    try:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        dag_run = ctx.get('dag_run')
        return dag_run.conf if dag_run and dag_run.conf else {}
    except Exception:
        return {}


@task(task_id='select_setup_tables')
def select_setup_tables() -> List[Dict]:
    """
    Sélectionne les tables à initialiser pour le setup.

    En mode standalone : traite les deux schémas (actif + inactif), en détectant
    si le schéma inactif existe sous son nom canonique ou avec le suffixe _offline.
    Quand déclenché par la DAG principale : utilise uniquement target_schema depuis conf.

    Returns:
        Liste de dicts de configuration de tables (format amue_tables),
        chacun enrichi avec 'target_schema'. En standalone : N tables × 2 schémas.
    """
    conf = _read_dag_run_conf()
    if target_schema := (conf.get('target_schema') if conf else None):
        schemas = [target_schema]
        logger.info(f"[SETUP] Déclenché par la DAG principale — schéma cible: {target_schema}")
    else:
        manager = BlueGreenManager()

        # Schéma actif : vérification physique (peut être _offline après sync)
        active_canonical = manager.get_active_schema()
        active_offline = active_canonical + BlueGreenManager.OFFLINE_SUFFIX
        if manager.schema_exists(active_canonical):
            active = active_canonical
        elif manager.schema_exists(active_offline):
            active = active_offline
        else:
            active = active_canonical  # premier lancement : sera créé par init DB

        # Schéma inactif
        inactive_canonical = manager.get_target_schema()
        inactive_offline = inactive_canonical + BlueGreenManager.OFFLINE_SUFFIX
        if manager.schema_exists(inactive_canonical):
            inactive = inactive_canonical
        elif manager.schema_exists(inactive_offline):
            inactive = inactive_offline
        else:
            inactive = inactive_canonical  # ne devrait pas arriver per garantie architecture

        schemas = [active, inactive]
        logger.info(f"[SETUP] Mode standalone — schémas: {schemas}")

    tables = TableConfigManager().get_tables_config()
    enabled = [t for t in tables if t.get('enable', False)]

    result = []
    for schema in schemas:
        for t in enabled:
            result.append({**t, 'target_schema': schema})

    logger.info(
        f"[SETUP] {len(result)} entrée(s) à traiter "
        f"({len(enabled)} table(s) × {len(schemas)} schéma(s))"
    )
    for schema in schemas:
        logger.debug(f"  Schéma {schema}:")
        for t in enabled:
            logger.debug(f"    - {t.get('table_name')} (setup_status={t.get('setup_status', 'pending')})")

    return result
