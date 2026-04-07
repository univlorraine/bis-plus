# common/notifications/callbacks_utils.py
"""Fonctions utilitaires partagées pour les callbacks Airflow de notification."""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def enrich_context_with_failed_tasks(context: Dict[str, Any], dag_label: str = '') -> None:
    """
    Enrichit le contexte Airflow avec la liste des tâches en échec.

    Appelé au niveau DAG (on_failure_callback) quand ``exception`` est absent
    du contexte (cas DAG-level vs task-level).

    Args:
        context:   Contexte Airflow (modifié en place).
        dag_label: Libellé du système pour le message de fallback (ex: ``'ECC'``).
    """
    if context.get('exception'):
        return  # contexte task-level : exception déjà présente, rien à enrichir

    dag_run = context.get('dag_run')
    if not dag_run:
        return

    try:
        failed_tis = dag_run.get_task_instances(state='failed')
    except Exception:
        logger.debug("dag_run.get_task_instances() non disponible — enrichissement ignoré")
        failed_tis = []

    if failed_tis:
        failed_names = [
            f"{ti.task_id}[{ti.map_index}]" if getattr(ti, 'map_index', -1) >= 0 else ti.task_id
            for ti in failed_tis
        ]
        context.setdefault('error_message', f"Tâches en échec : {', '.join(failed_names)}")
        context['failed_tasks'] = [
            {
                'task_id': ti.task_id,
                'map_index': getattr(ti, 'map_index', -1),
                'duration': round(ti.duration, 1) if getattr(ti, 'duration', None) else None,
            }
            for ti in failed_tis
        ]
    else:
        prefix = f"DAG {dag_label}" if dag_label else "DAG"
        context.setdefault(
            'error_message',
            f"Le {prefix} a échoué — consulter les logs des tâches pour le détail"
        )

    context.setdefault('error_type', 'DAGFailure')
