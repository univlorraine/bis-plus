# ecc/tasks/import_dag/send_report.py
"""Task d'envoi du rapport d'import ECC."""
import logging
from datetime import datetime
from typing import Dict, List

from airflow.sdk import task, get_current_context

logger = logging.getLogger(__name__)


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"


@task(task_id='send_ecc_report', multiple_outputs=False)
def send_ecc_report(import_results: List[Dict]) -> Dict:
    """
    Génère et envoie le rapport d'import ECC.

    Args:
        import_results: Liste des résultats d'import par table

    Returns:
        Rapport généré avec métriques agrégées
    """
    logger.info("[ECC] Génération rapport d'import")

    total_fetched = sum(r.get('rows_fetched', 0) for r in import_results)
    total_inserted = sum(r.get('rows_inserted', 0) for r in import_results)
    total_updated = sum(r.get('rows_updated', 0) for r in import_results)
    total_skipped = sum(r.get('rows_skipped', 0) for r in import_results)
    tables_ok = sum(1 for r in import_results if r.get('status') == 'success')

    logger.info(
        f"[ECC] Résumé: {len(import_results)} tables, "
        f"{total_fetched} récupérées, {total_inserted} insérées, "
        f"{total_updated} mises à jour, {total_skipped} protégées (sifac_plus)"
    )

    ctx = get_current_context()
    dag_run = ctx.get('dag_run')
    start_date = dag_run.start_date if dag_run else None
    duration = (
        _format_duration((datetime.now(tz=start_date.tzinfo) - start_date).total_seconds())
        if start_date else 'N/A'
    )

    from ecc.notifications.ecc_notifier import ECCNotificationService
    service = ECCNotificationService()
    service.notify_success({
        'dag_id': 'ecc_multi_table_import',
        'tables_imported': import_results,
        'title': 'Import ECC Réussi',
        'duration': duration,
    })

    return {
        'ecc_summary': {
            'tables_processed': len(import_results),
            'tables_success': tables_ok,
            'total_fetched': total_fetched,
            'total_inserted': total_inserted,
            'total_updated': total_updated,
            'total_skipped': total_skipped,
        }
    }
