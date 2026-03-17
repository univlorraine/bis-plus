"""Task de sélection des tables après polling API."""
import logging
from typing import Dict, List

from airflow.sdk import task
from airflow.sdk import get_current_context

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.services.api.status_checker import AMUEStatusChecker
from amue.operators.table_management.table_filter import AMUETableFilter

logger = logging.getLogger(__name__)


@task(task_id='select_tables')
def select_tables(bluegreen_ctx: Dict) -> List[Dict]:
    """
    Sélection des tables après polling.

    Récupère le polling_result depuis XCom du sensor wait_for_api.

    Args:
        bluegreen_ctx: Contexte blue/green de init_bluegreen()

    Returns:
        Liste de dictionnaires contenant les infos de chaque table.
    """
    logger.info("[INIT] API prête, sélection des tables...")

    # Récupère le polling_result depuis XCom du sensor (return_value ou clé custom)
    context = get_current_context()
    polling_result = (
        context['ti'].xcom_pull(task_ids='wait_for_api')
        or context['ti'].xcom_pull(task_ids='wait_for_api', key='polling_result')
        or {}
    )

    current_status = polling_result.get('tables_status')
    if current_status is None:
        logger.warning("[INIT] tables_status non disponible, appel API de secours")
        current_status = {}
        api_hook = AMUEAPIHook()
        status_checker = AMUEStatusChecker(api_hook)
        current_status = status_checker.get_current_status()
    else:
        logger.info(f"[INIT] Utilisation du cache tables_status ({len(current_status)} tables)")

    table_filter = AMUETableFilter()
    tables = table_filter.filter_tables(current_status)

    # Injecte le schéma cible blue/green dans chaque table
    target_schema = bluegreen_ctx.get("target_schema") if bluegreen_ctx.get("enabled") else None
    for t in tables:
        t["target_schema"] = target_schema

    if not tables:
        logger.info("[INIT] Aucune table à importer")
    else:
        logger.info(f"[INIT] {len(tables)} table(s) à importer")
        if target_schema:
            logger.info(f"[INIT] Schéma cible: {target_schema}")
        for t in tables:
            logger.info(f"  - {t.get('name')}")

    return tables


