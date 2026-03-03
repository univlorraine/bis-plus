"""Task de vérification d'une table avant import."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.operators.table_management.table_verifier import AMUETableVerifier

logger = logging.getLogger(__name__)


@task(task_id='verify_table')
def verify_table(table_info: Dict) -> Dict:
    """
    Vérifie une table avant import.

    Cette task est exécutée en parallèle pour chaque table (via .expand()).
    Elle vérifie que :
        - La table est disponible côté API (statut OK)
        - La structure n'a pas changé (comparaison fingerprint)
        - Les colonnes sont valides

    Args:
        table_info: Dictionnaire avec les infos de la table

    Returns:
        Résultat de vérification avec status, columns, fingerprint, etc.
    """
    api_hook = AMUEAPIHook()
    target_schema = table_info.get("target_schema")
    verifier = AMUETableVerifier(api_hook, target_schema=target_schema)
    result = verifier.verify_table(table_info)
    # Propage le schéma cible dans le résultat
    result["target_schema"] = target_schema
    # Retire type_original (non utilisé en aval) pour réduire la payload XCom
    for col in result.get("columns", []):
        col.pop("type_original", None)
    return result
