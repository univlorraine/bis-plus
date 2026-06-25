"""Tasks du DAG de découverte des tables AMUE (depuis le statut API)."""
from amue.tasks.discovery_dag.discover_tables import discover_tables
from amue.tasks.discovery_dag.register_tables import register_tables
from amue.tasks.discovery_dag.trigger_setup_if_needed import trigger_setup_if_needed
from amue.tasks.discovery_dag.send_discovery_report import send_discovery_report

__all__ = ['discover_tables', 'register_tables', 'trigger_setup_if_needed', 'send_discovery_report']
