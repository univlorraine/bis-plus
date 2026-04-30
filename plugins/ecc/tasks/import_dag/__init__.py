"""Tasks du DAG d'import ECC."""
from ecc.tasks.import_dag.select_tables import select_tables
from ecc.tasks.import_dag.import_data import import_data
from ecc.tasks.import_dag.sync_to_active import sync_to_active
from ecc.tasks.import_dag.save_metadata import save_metadata
from ecc.tasks.import_dag.send_report import send_report

__all__ = [
    'select_tables',
    'import_data',
    'sync_to_active',
    'save_metadata',
    'send_report',
]
