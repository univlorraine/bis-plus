# ecc/tasks/import_dag/send_report.py
"""Task d'envoi du rapport d'import ECC."""
import logging
from datetime import datetime
from typing import Dict, List

from airflow.sdk import task

logger = logging.getLogger(__name__)


@task(task_id='send_ecc_report')
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

    # Calcul de la durée depuis le début du DAG run
    duration = 'N/A'
    execution_date = datetime.now().isoformat()
    try:
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        dag_run = ctx.get('dag_run')
        if dag_run and dag_run.start_date:
            elapsed = datetime.now(dag_run.start_date.tzinfo) - dag_run.start_date
            total_seconds = int(elapsed.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            if hours:
                duration = f"{hours}h {minutes:02d}m {seconds:02d}s"
            else:
                duration = f"{minutes}m {seconds:02d}s"
            execution_date = dag_run.start_date.isoformat()
    except Exception as dur_err:
        logger.warning(f"[ECC] Impossible de calculer la durée: {dur_err}")

    from ecc.notifications.ecc_notifier import ECCNotificationService

    service = ECCNotificationService()
    service.notify_success({
        'dag_id': 'ecc_multi_table_import',
        'execution_date': execution_date,
        'duration': duration,
        'tables_imported': import_results,
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
