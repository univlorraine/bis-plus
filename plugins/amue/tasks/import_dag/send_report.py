"""Task de génération et envoi du rapport d'exécution."""
import json
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.notifications.report_generator import AMUEReportGenerator
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@task(task_id='send_report')
def send_report(import_results: List[Dict], switch_result: Dict) -> Dict:
    """
    Génère et envoie le rapport d'exécution par email.

    Args:
        import_results: Liste des résultats de import_data()
        switch_result: Résultat de switch_views()

    Returns:
        Statut de l'envoi : {"sent": True/False, "recipients": [...]}
    """
    polling_json = VarMgr.get('_current_run_polling', default='{}')
    try:
        polling_result = json.loads(polling_json)
    except Exception:
        polling_result = {}

    generator = AMUEReportGenerator()
    return generator.generate_and_send(import_results, polling_result)
