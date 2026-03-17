"""
Tasks du DAG d'import AMUE

Fonctions @task extraites de dag_amue_dynamic_table.py
pour séparer orchestration et logique métier.
"""
from amue.tasks.import_dag.init_bluegreen import init_bluegreen
from amue.tasks.import_dag.polling import select_tables
from amue.tasks.import_dag.check_setup_status import check_setup_status
from amue.tasks.import_dag.import_data import import_data
from amue.tasks.import_dag.save_metadata import save_metadata
from amue.tasks.import_dag.switch_views import switch_views
from amue.tasks.import_dag.send_report import send_report

__all__ = [
    'init_bluegreen',
    'select_tables',
    'check_setup_status',
    'import_data',
    'save_metadata',
    'switch_views',
    'send_report',
]
