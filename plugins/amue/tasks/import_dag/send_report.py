"""Task de génération et envoi du rapport d'exécution."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.notifications.report_generator import AMUEReportGenerator
from amue.services.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


@task(task_id='send_report', multiple_outputs=False)
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
    blocked = [
        t['table_name'] for t in TableConfigManager().get_tables_config()
        if t.get('setup_status') == 'blocked'
    ]
    if blocked:
        logger.error(
            f"[SEND_REPORT] {len(blocked)} table(s) bloquée(s) — structure modifiée : "
            f"{', '.join(blocked)}. Réimport manuel ou reset des fingerprints requis."
        )

    generator = AMUEReportGenerator()
    return generator.generate_and_send(import_results, polling_result or {})
