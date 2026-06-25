"""Task d'envoi du rapport d'import ECC."""
import logging
from datetime import datetime
from typing import Dict, List

from airflow.sdk import task, get_current_context

from common.infrastructure.observability.log_prefixes import LogPrefixes
from common.tasks.import_summary import summarize_import_results

logger = logging.getLogger(__name__)


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"


@task(task_id='send_ecc_report', multiple_outputs=False)
def send_report(import_results: List[Dict]) -> Dict:
    """
    Génère et envoie le rapport d'import ECC.

    Args:
        import_results: Liste des résultats d'import par table

    Returns:
        Rapport généré avec métriques agrégées
    """
    logger.info(f"{LogPrefixes.ECC_REPORT} Génération rapport d'import")

    summary = summarize_import_results(import_results)

    logger.info(
        f"{LogPrefixes.ECC_REPORT} Résumé: {summary['tables_processed']} tables, "
        f"{summary['total_fetched']} récupérées, {summary['total_inserted']} insérées, "
        f"{summary['total_updated']} mises à jour, {summary['total_skipped']} protégées (sifac_plus)"
    )

    ctx = get_current_context()
    dag_run = ctx.get('dag_run')
    start_date = dag_run.start_date if dag_run else None
    duration = (
        _format_duration((datetime.now(tz=start_date.tzinfo) - start_date).total_seconds())
        if start_date else 'N/A'
    )

    from ecc.infrastructure.notifications.ecc_notifier import ECCNotificationService
    service = ECCNotificationService()
    service.notify_success({
        'dag_id': 'ecc_multi_table_import',
        'tables_imported': import_results,
        'title': 'Import ECC Réussi',
        'duration': duration,
    })

    return {'ecc_summary': summary}
