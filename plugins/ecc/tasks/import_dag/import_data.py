"""Task d'import Oracle → PostgreSQL pour une table ECC."""
import logging
from typing import Dict

from airflow.sdk import task

from ecc.operators.pipeline.ecc_data_importer import ECCDataImporter
from ecc.utils.config.settings import get_ecc_batch_size

logger = logging.getLogger(__name__)


@task(task_id='import_ecc_data', multiple_outputs=False)
def import_data(table_config: Dict) -> Dict:
    """
    Importe une table Oracle SAP ECC vers PostgreSQL via UPSERT.

    Protection sifac_plus : si la ligne en conflit a _source='sifac_plus',
    elle n'est pas remplacée (guard WHERE dans le DO UPDATE SET).

    Args:
        table_config: Config table depuis select_tables() :
            {table_name, ecc_query, primary_keys (list), target_schema,
             source, protected_source}

    Returns:
        {table_name, rows_fetched, rows_inserted, rows_updated, rows_skipped,
         status, target_schema, import_type}
    """
    importer = ECCDataImporter(
        target_schema=table_config['target_schema'],
        source=table_config.get('source', 'ecc'),
        protected_source=table_config.get('protected_source', 'sifac_plus'),
    )
    return importer.import_table(
        table_name=table_config['table_name'],
        ecc_query=table_config['ecc_query'],
        primary_keys=table_config['primary_keys'],
        batch_size=get_ecc_batch_size(),
    )
