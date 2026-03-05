"""Tasks du DAG d'import ECC."""
from ecc.tasks.import_dag.select_tables import select_ecc_tables
from ecc.tasks.import_dag.import_data import import_ecc_data
from ecc.tasks.import_dag.send_report import send_ecc_report

__all__ = [
    'select_ecc_tables',
    'import_ecc_data',
    'send_ecc_report',
]
