"""Tasks du DAG d'import ECC."""
from ecc.tasks.import_dag.select_tables import select_ecc_tables
from ecc.tasks.import_dag.import_data import import_ecc_data
from ecc.tasks.import_dag.sync_to_active import sync_ecc_to_active
from ecc.tasks.import_dag.save_metadata import save_ecc_metadata
from ecc.tasks.import_dag.send_report import send_ecc_report

__all__ = [
    'select_ecc_tables',
    'import_ecc_data',
    'sync_ecc_to_active',
    'save_ecc_metadata',
    'send_ecc_report',
]
