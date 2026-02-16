"""Task de sauvegarde des métadonnées après import."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.services.metadata_manager import AMUEMetadataManager

logger = logging.getLogger(__name__)


@task(task_id='save_metadata')
def save_metadata(import_results: List[Dict], polling_result: Dict) -> Dict:
    """
    Met à jour les métadonnées après un import réussi.

    Pour chaque table importée avec succès :
        - Sauvegarde le nouveau fingerprint
        - Enregistre la date de dernier import
        - Sauvegarde le finish timestamp pour le prochain polling

    Args:
        import_results: Liste des résultats de import_data()
        polling_result: Résultat du polling (via XCom)

    Returns:
        Contexte pour les phases suivantes (blue/green, etc.)
    """
    finish_timestamp = polling_result.get('finish', '') if polling_result else ''

    manager = AMUEMetadataManager()
    manager.update_metadata(import_results, finish_timestamp=finish_timestamp)
    logger.info(f"[METADATA] Métadonnées mises à jour pour {len(import_results)} table(s)")

    target_schema = None
    if import_results:
        target_schema = import_results[0].get('target_schema')

    return {
        'tables_imported': len(import_results),
        'target_schema': target_schema
    }
