"""
Tasks du DAG d'import AMUE

Fonctions @task extraites de dag_amue_dynamic_table.py
pour séparer orchestration et logique métier.
"""
from amue.tasks.import_dag.init_bluegreen import init_bluegreen
from amue.tasks.import_dag.polling import wait_for_api_and_select
from amue.tasks.import_dag.verify_table import verify_table
from amue.tasks.import_dag.validate_tables import validate_tables
from amue.tasks.import_dag.prepare_table import prepare_table
from amue.tasks.import_dag.import_data import import_data
from amue.tasks.import_dag.save_metadata import save_metadata
from amue.tasks.import_dag.switch_views import switch_views
from amue.tasks.import_dag.send_report import send_report

__all__ = [
    'init_bluegreen',
    'wait_for_api_and_select',
    'verify_table',
    'validate_tables',
    'prepare_table',
    'import_data',
    'save_metadata',
    'switch_views',
    'send_report',
]
