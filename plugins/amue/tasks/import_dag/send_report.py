"""Task de génération et envoi du rapport d'exécution."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.notifications.report_generator import AMUEReportGenerator

logger = logging.getLogger(__name__)


@task(task_id='send_report')
def send_report(import_results: List[Dict], switch_result: Dict, polling_result: Dict) -> Dict:
    """
    Génère et envoie le rapport d'exécution par email.

    Args:
        import_results: Liste des résultats de import_data()
        switch_result: Résultat de switch_views()
        polling_result: Résultat du polling (via XCom)

    Returns:
        Statut de l'envoi : {"sent": True/False, "recipients": [...]}
    """
    generator = AMUEReportGenerator()
    return generator.generate_and_send(import_results, polling_result or {})
