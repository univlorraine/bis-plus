"""Task de setup d'une table AMUE — wrapper de TableSetupOrchestrator."""
from typing import Dict

from airflow.sdk import task

from amue.application.table_setup_orchestrator import TableSetupOrchestrator


@task(task_id='setup_table', multiple_outputs=False)
def setup_table(table_info: Dict) -> Dict:
    """
    Initialise ou vérifie une table AMUE.

    Exécutée en parallèle pour chaque table (via .expand()).
    Délègue toute la logique à TableSetupOrchestrator.

    Args:
        table_info: Configuration de la table (format TableConfigManager.get_tables_config())

    Returns:
        {table_name, status, setup_status, created, columns_count, error}
    """
    return TableSetupOrchestrator().run(table_info)
