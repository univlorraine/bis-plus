"""Task de préparation de la structure PostgreSQL pour l'import."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.operators.table_management.table_manager import AMUETableManager

logger = logging.getLogger(__name__)


@task(task_id='prepare_table')
def prepare_table(verified_table: Dict) -> Dict:
    """
    Prépare la structure PostgreSQL pour l'import.

    En mode DÉVELOPPEMENT : crée/modifie la table si nécessaire.
    En mode PRODUCTION : vérifie que la table existe.

    Args:
        verified_table: Résultat de verify_table() avec colonnes et fingerprint

    Returns:
        Dictionnaire enrichi avec les infos pour l'import.
    """
    target_schema = verified_table.get("target_schema")
    manager = AMUETableManager(target_schema=target_schema)
    result = manager.manage_table(verified_table)

    result['original_info'] = verified_table.get('original_info', {})
    result['target_schema'] = target_schema
    return result
