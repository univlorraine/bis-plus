"""Task d'import des données depuis l'API vers PostgreSQL."""
import logging
from datetime import timedelta
from typing import Dict

from airflow.sdk import task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.operators.pipeline.data_importer import AMUEDataImporter
from amue.utils.database.hooks import create_postgres_hook
from common.logging_context import set_correlation_id

logger = logging.getLogger(__name__)

_COLUMNS_SQL = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
      AND column_name NOT IN ('_source', '_imported_at')
    ORDER BY ordinal_position
"""


@task(task_id='import_data', retries=2, retry_delay=timedelta(seconds=30), multiple_outputs=False)
def import_data(table_info: Dict) -> Dict:
    """
    Importe les données d'une table depuis l'API vers PostgreSQL.

    Récupère les colonnes depuis information_schema puis importe via l'API.

    Stratégie d'import :
        - Si clé primaire définie : UPSERT (INSERT ON CONFLICT UPDATE)
        - Sinon : INSERT simple (truncate + insert)

    Args:
        table_info: Configuration de la table (format check_setup_status())

    Returns:
        Résultat de l'import avec status, rows_imported, etc.
    """
    table_name = table_info.get('name', 'unknown').lower()
    target_schema = table_info.get('target_schema')
    primary_keys_str = table_info.get('primary_key', '')

    # Propage le correlation_id par table pour le tracing granulaire
    set_correlation_id(f"import-{table_name[:12]}")

    hook = create_postgres_hook(bluegreen_schema=target_schema) if target_schema else create_postgres_hook()
    schema = target_schema or 'splus'
    rows = hook.get_records(_COLUMNS_SQL, parameters=(schema, table_name))
    if not rows:
        raise Exception(
            f"Aucune colonne trouvée pour {schema}.{table_name} — "
            f"la table a-t-elle été initialisée par amue_table_setup ?"
        )
    columns = [row[0] for row in rows]
    logger.info(f"[IMPORT] {table_name}: {len(columns)} colonne(s) récupérées depuis information_schema")

    api_hook = AMUEAPIHook()
    importer = AMUEDataImporter(api_hook, target_schema=target_schema)

    primary_keys = [pk.strip() for pk in primary_keys_str.split(',') if pk.strip()]

    result = importer.import_table(
        table_name=table_name,
        columns=columns,
        primary_keys=primary_keys,
        import_config=table_info,
    )

    result['target_schema'] = target_schema
    return result
