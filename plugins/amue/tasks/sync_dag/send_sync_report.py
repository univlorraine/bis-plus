"""Task d'envoi du rapport de synchronisation blue/green."""
import logging
from datetime import datetime
from typing import Dict

from airflow.sdk import task

from amue.notifications import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='send_sync_report', multiple_outputs=False)
def send_sync_report(sync_result: Dict) -> Dict:
    """
    Envoie un email de résumé de la synchronisation blue/green.

    Args:
        sync_result: Résultat retourné par run_sync()

    Returns:
        Dict avec statut d'envoi {"sent": bool}
    """
    status = sync_result.get('status', 'unknown')

    if status == 'skipped':
        logger.info("[SYNC] Rapport ignoré (synchronisation désactivée)")
        return {'sent': False, 'reason': 'skipped'}

    tables_synced = sync_result.get('tables_synced', 0)
    tables_failed = sync_result.get('tables_failed', 0)
    total_rows = sync_result.get('total_rows_copied', 0)
    source = sync_result.get('source_schema', '?')
    target = sync_result.get('target_schema', '?')

    service = NotificationService()
    try:
        if status in ('success', 'partial'):
            sent = service.notify_sync_success({
                'dag_id': 'amue_sync_schemas',
                'execution_date': datetime.now().isoformat(),
                'sync_source': source,
                'sync_target': target,
                'tables_failed': tables_failed,
                'tables_imported': [
                    {
                        'table_name': d.get('table_name', '?'),
                        'rows_inserted': d.get('rows_copied', 0),
                        'status': d.get('status', 'success'),
                    }
                    for d in sync_result.get('details', [])
                    if d.get('status') == 'success'
                ],
            })
        else:
            details = sync_result.get('details', [])
            errors = [
                f"{d.get('table_name', '?')}: {d.get('error', '')}"
                for d in details if d.get('status') == 'error'
            ]
            sent = service.notify_error({
                'dag_id': 'amue_sync_schemas',
                'task_id': 'run_sync',
                'error_message': f"Sync échouée ({tables_failed} tables en erreur). " + ' | '.join(errors),
                'error_type': 'SyncError',
            })

        logger.info(f"[SYNC] Rapport envoyé: {sent}")
        return {'sent': sent, 'recipients': service.recipients}
    except Exception as e:
        logger.error(f"[SYNC] Impossible d'envoyer le rapport: {e}")
        return {'sent': False, 'error': str(e)}
