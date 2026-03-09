"""Tasks du DAG de setup des tables AMUE."""
from amue.tasks.setup_dag.select_setup_tables import select_setup_tables
from amue.tasks.setup_dag.setup_table import setup_table
from amue.tasks.setup_dag.send_setup_report import send_setup_report

__all__ = ['select_setup_tables', 'setup_table', 'send_setup_report']
