"""Task d'import des données depuis l'API vers PostgreSQL."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.operators.pipeline.data_importer import AMUEDataImporter

logger = logging.getLogger(__name__)


@task(task_id='import_data')
def import_data(prepared_table: Dict) -> Dict:
    """
    Importe les données d'une table depuis l'API vers PostgreSQL.

    Stratégie d'import :
        - Si clé primaire définie : UPSERT (INSERT ON CONFLICT UPDATE)
        - Sinon : INSERT simple (truncate + insert)

    Args:
        prepared_table: Résultat de prepare_table()

    Returns:
        Résultat de l'import avec status, rows_imported, etc.
    """
    api_hook = AMUEAPIHook()
    target_schema = prepared_table.get("target_schema")
    importer = AMUEDataImporter(api_hook, target_schema=target_schema)

    primary_keys = [
        pk.strip()
        for pk in prepared_table['primary_keys'].split(',')
        if pk.strip()
    ]

    result = importer.import_table(
        table_name=prepared_table['table_name'],
        columns=prepared_table['columns'],
        primary_keys=primary_keys,
        import_config=prepared_table['original_info']
    )

    result['target_schema'] = target_schema
    return result
