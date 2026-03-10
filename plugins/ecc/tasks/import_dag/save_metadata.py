"""Task de sauvegarde des métadonnées après import ECC (une entrée par table)."""
import logging
from datetime import datetime
from typing import Dict

from airflow.sdk import task

logger = logging.getLogger(__name__)


@task(task_id='save_ecc_metadata')
def save_ecc_metadata(import_result: Dict) -> Dict:
    """
    Enregistre les métadonnées d'un import ECC pour une table.

    Permet la traçabilité des imports ECC (audit trail) sans bloquer
    le pipeline si la sauvegarde échoue.

    Args:
        import_result: Résultat d'import_ecc_data() pour une table :
            {table_name, rows_fetched, rows_inserted, rows_updated, status, ...}

    Returns:
        {table_name, import_timestamp, rows_imported}
    """
    table_name = import_result.get('table_name', 'unknown')
    rows_fetched = import_result.get('rows_fetched', 0)
    status = import_result.get('status', 'unknown')
    import_timestamp = datetime.now().isoformat()

    if status == 'success':
        logger.info(f"[ECC_METADATA] {table_name}: {rows_fetched} lignes importées à {import_timestamp}")
    else:
        logger.warning(f"[ECC_METADATA] {table_name}: import en erreur (status={status})")

    return {
        'table_name': table_name,
        'import_timestamp': import_timestamp,
        'rows_imported': rows_fetched,
    }
