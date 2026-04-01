"""Task de rapport du setup des tables AMUE."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.notifications.notifier import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='send_setup_report', multiple_outputs=False)
def send_setup_report(setup_results: List[Dict]) -> Dict:
    """
    Génère un rapport du setup et envoie une alerte si des tables sont bloquées.

    Args:
        setup_results: Liste des résultats de setup_table()

    Returns:
        Résumé : {tables_ready, tables_blocked, tables_created, tables_error}
    """
    tables_ready = [r for r in setup_results if r.get('setup_status') == 'ready' and r.get('status') != 'error']
    tables_blocked = [r for r in setup_results if r.get('setup_status') == 'blocked']
    tables_error = [r for r in setup_results if r.get('status') == 'error' and r.get('setup_status') != 'blocked']
    tables_created = [r for r in tables_ready if r.get('created')]

    logger.info(f"[SETUP_REPORT] Résultat du setup:")
    logger.info(f"  - Prêtes   : {len(tables_ready)}")
    logger.info(f"  - Créées   : {len(tables_created)}")
    logger.info(f"  - Bloquées : {len(tables_blocked)}")
    logger.info(f"  - Erreurs  : {len(tables_error)}")

    for t in tables_blocked:
        name = t['table_name']
        fp_api_changed = t.get('fp_api_changed')
        fp_ul_changed = t.get('fp_ul_changed')
        cols = t.get('columns_count')
        sep = '═' * (40 - len(name))
        logger.error(f"[SETUP_REPORT] ══ BLOQUÉE : {name} {sep}")
        if cols is not None:
            logger.error(f"[SETUP_REPORT]   Colonnes détectées : {cols}")
        if fp_api_changed is not None and fp_ul_changed is not None:
            api_label = 'MODIFIÉ  → structure côté serveur AMUE' if fp_api_changed else 'inchangé'
            ul_label = 'MODIFIÉ  → config locale (PKs / types PG)' if fp_ul_changed else 'inchangé'
            logger.error(f"[SETUP_REPORT]   fingerprint_API    : {api_label}")
            logger.error(f"[SETUP_REPORT]   fingerprint_UL     : {ul_label}")
            if fp_api_changed and fp_ul_changed:
                cause = "colonnes ajoutées/supprimées (API + UL affectés)"
            elif fp_api_changed:
                cause = "types ou colonnes côté API uniquement"
            else:
                cause = "clés primaires UL ou types PG modifiés (config locale)"
            logger.error(f"[SETUP_REPORT]   → Cause probable   : {cause}")
            if fp_ul_changed:
                ul_diff = t.get('ul_diff')
                if ul_diff:
                    logger.error(f"[SETUP_REPORT]   Diff colonnes (PG existant → API) :")
                    for line in ul_diff.splitlines():
                        logger.error(f"[SETUP_REPORT]     {line}")
                else:
                    logger.error(f"[SETUP_REPORT]   (table absente en PG — diff non disponible)")
        else:
            # Fallback : parser le champ error ligne par ligne
            for line in t.get('error', 'structure modifiée').splitlines():
                logger.error(f"[SETUP_REPORT]   {line.strip()}")
        logger.error(f"[SETUP_REPORT]   → Action requise   : relancer amue_table_setup après vérification")

    for t in tables_error:
        name = t['table_name']
        sep = '═' * (41 - len(name))
        logger.error(f"[SETUP_REPORT] ══ ERREUR : {name} {sep}")
        logger.error(f"[SETUP_REPORT]   {t.get('error')}")

    if tables_blocked or tables_error:
        try:
            NotificationService().notify_setup_error({
                'dag_id': 'amue_table_setup',
                'tables_blocked': [
                    {
                        'table_name': t['table_name'],
                        'fp_api_changed': t.get('fp_api_changed'),
                        'fp_ul_changed': t.get('fp_ul_changed'),
                        'columns_count': t.get('columns_count'),
                        'ul_diff': t.get('ul_diff', ''),
                        'error': t.get('error', ''),
                    }
                    for t in tables_blocked
                ],
                'tables_error': [
                    {
                        'table_name': t['table_name'],
                        'error': t.get('error', ''),
                    }
                    for t in tables_error
                ],
            })
        except Exception as e:
            logger.warning(f"[SETUP_REPORT] Envoi notification échoué: {e}")

    return {
        'tables_ready': len(tables_ready),
        'tables_blocked': len(tables_blocked),
        'tables_created': len(tables_created),
        'tables_error': len(tables_error),
    }
