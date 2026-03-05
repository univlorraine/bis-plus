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
    Détermine le schéma actif via BlueGreenManager (ECC insère directement
    dans le schéma actif, sans switch de schéma).

    Returns:
        Liste de dicts par table :
        {
            'table_name': str,
            'ecc_query': str,
            'primary_keys': List[str],
            'target_schema': str,       ← schéma actif (ex: 'splus_blue')
            'source': str,
            'protected_source': str
        }
    """
    active_schema = BlueGreenManager().get_active_schema()
    logger.info(f"[ECC] Schéma actif pour l'import: {active_schema}")

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

    tables = []
    for table_name, ecc_query, primary_key in rows:
        pk_list = [pk.strip() for pk in primary_key.split(',') if pk.strip()]
        tables.append({
            'table_name': table_name,
            'ecc_query': ecc_query,
            'primary_keys': pk_list,
            'target_schema': active_schema,
            'source': ECCDefaults.SOURCE_NAME,
            'protected_source': ECCDefaults.PROTECTED_SOURCE,
        })
        logger.info(f"[ECC] Table sélectionnée: {table_name} (PKs: {pk_list})")

    logger.info(f"[ECC] {len(tables)} table(s) sélectionnée(s) → {active_schema}")
    return tables
