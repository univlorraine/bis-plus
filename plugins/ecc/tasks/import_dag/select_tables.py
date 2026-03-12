# ecc/tasks/import_dag/select_tables.py
"""Task de sélection des tables ECC depuis splus_admin.amue_tables (colonne ecc_query)."""
import logging
from typing import List, Dict

from airflow.sdk import task

from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
from amue.utils.database.hooks import create_postgres_hook
from ecc.utils.config.settings import ECCDefaults

logger = logging.getLogger(__name__)


@task(task_id='select_ecc_tables')
def select_ecc_tables() -> List[Dict]:
    """
    Lit la liste des tables ECC activées depuis splus_admin.amue_tables.

    Sélectionne les lignes dont ecc_query est non-NULL et non-vide.
    Détermine le schéma actif et le schéma inactif via BlueGreenManager.
    ECC importe dans les deux schémas pour garantir la cohérence après un
    switch Blue/Green déclenché par AMUE.

    Returns:
        Liste de dicts par table × schéma :
        {
            'table_name': str,
            'ecc_query': str,
            'primary_keys': List[str],
            'target_schema': str,       ← schéma actif ou inactif
            'source': str,
            'protected_source': str
        }
    """
    manager = BlueGreenManager()
    active = manager.get_active_schema()

    inactive_canonical = manager.get_target_schema()
    inactive_offline = inactive_canonical + BlueGreenManager.OFFLINE_SUFFIX

    if manager.schema_exists(inactive_canonical):
        inactive = inactive_canonical
    elif manager.schema_exists(inactive_offline):
        inactive = inactive_offline
    else:
        inactive = None  # premier lancement

    schemas = [s for s in [active, inactive] if s]
    logger.info(f"[ECC] Import dans les schémas: {schemas}")

    pg_hook = create_postgres_hook(schema='splus_admin')
    rows = pg_hook.get_records(
        """
        SELECT table_name, ecc_query, primary_key
        FROM splus_admin.amue_tables
        WHERE enabled = TRUE
          AND ecc_query IS NOT NULL
          AND ecc_query != ''
        ORDER BY table_name
        """
    )

    if not rows:
        logger.warning("[ECC] Aucune table ECC activée dans splus_admin.amue_tables (ecc_query non-NULL)")
        return []

    result = []
    for schema in schemas:
        for table_name, ecc_query, primary_key in rows:
            pk_list = [pk.strip() for pk in primary_key.split(',') if pk.strip()]
            result.append({
                'table_name': table_name,
                'ecc_query': ecc_query,
                'primary_keys': pk_list,
                'target_schema': schema,
                'source': ECCDefaults.SOURCE_NAME,
                'protected_source': ECCDefaults.PROTECTED_SOURCE,
            })
            logger.debug(f"[ECC] Table sélectionnée: {table_name} → {schema} (PKs: {pk_list})")

    logger.info(f"[ECC] {len(result)} entrée(s) ({len(rows)} table(s) × {len(schemas)} schéma(s))")
    return result
