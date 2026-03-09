"""Task de rapport du setup des tables AMUE."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.notifications.notifier import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='send_setup_report')
def send_setup_report(setup_results: List[Dict]) -> Dict:
    """
    Génère un rapport du setup et envoie une alerte si des tables sont bloquées.

    Args:
        setup_results: Liste des résultats de setup_table()

    Returns:
        Résumé : {tables_ready, tables_blocked, tables_created, tables_error}
    """
    tables_ready = [r for r in setup_results if r.get('setup_status') == 'ready']
    tables_blocked = [r for r in setup_results if r.get('setup_status') == 'blocked']
    tables_error = [r for r in setup_results if r.get('status') == 'error' and r.get('setup_status') != 'blocked']
    tables_created = [r for r in tables_ready if r.get('created')]

    logger.info(f"[SETUP_REPORT] Résultat du setup:")
    logger.info(f"  - Prêtes   : {len(tables_ready)}")
    logger.info(f"  - Créées   : {len(tables_created)}")
    logger.info(f"  - Bloquées : {len(tables_blocked)}")
    logger.info(f"  - Erreurs  : {len(tables_error)}")

    for t in tables_blocked:
        logger.error(f"  [BLOCKED] {t['table_name']}: {t.get('error', 'structure modifiée')}")

    for t in tables_error:
        logger.error(f"  [ERROR] {t['table_name']}: {t.get('error')}")

    if tables_blocked or tables_error:
        blocked_names = [t['table_name'] for t in tables_blocked]
        error_names = [t['table_name'] for t in tables_error]
        problems = []
        if blocked_names:
            problems.append(f"Tables bloquées (changement de structure) : {', '.join(blocked_names)}")
        if error_names:
            problems.append(f"Tables en erreur : {', '.join(error_names)}")

        try:
            NotificationService().notify_error({
                'dag_id': 'amue_table_setup',
                'task_id': 'send_setup_report',
                'error_message': '\n'.join(problems),
                'error_type': 'SetupIncomplete',
            })
        except Exception as e:
            logger.warning(f"[SETUP_REPORT] Envoi notification échoué: {e}")

    return {
        'tables_ready': len(tables_ready),
        'tables_blocked': len(tables_blocked),
        'tables_created': len(tables_created),
        'tables_error': len(tables_error),
    }
