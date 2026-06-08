"""Task de sélection de tables pour un run de correction AMUE (sans sensor API)."""
import logging
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context, task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.operators.table_management.table_filter import AMUETableFilter
from amue.services.api.status_checker import AMUEStatusChecker
from amue.services.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


@task(task_id='select_tables_correction')
def select_tables_correction(bluegreen_ctx: Dict) -> List[Dict]:
    """
    Sélection de tables pour un run de correction manuelle.

    Lit la liste des tables depuis dag_run.conf['selected_tables'] (obligatoire).
    Appelle l'API AMUE directement sans attendre le sensor de polling.
    Seules les tables sélectionnées sont validées contre l'API.
    L'import sera toujours de type FULL (pas de delta pour les corrections).

    Args:
        bluegreen_ctx: Contexte blue/green de init_bluegreen()

    Returns:
        Liste de dicts contenant les infos de chaque table sélectionnée.
    """
    context = get_current_context()
    dag_run = context.get('dag_run')
    selected_tables = list((dag_run.conf or {}).get('selected_tables', [])) if dag_run else []

    if not selected_tables:
        raise AirflowException(
            "[CORRECTION] selected_tables est requis. "
            "Déclenchez via 'Trigger DAG w/ config' et sélectionnez les tables cibles."
        )

    logger.info(f"[CORRECTION] Tables demandées: {selected_tables}")

    # Charge la config et filtre sur la sélection uniquement
    all_config = TableConfigManager().get_tables_config()
    selected_lower = {t.lower() for t in selected_tables}
    filtered_config = [c for c in all_config if c['table_name'].lower() in selected_lower]

    unknown = selected_lower - {c['table_name'].lower() for c in filtered_config}
    if unknown:
        raise AirflowException(
            f"[CORRECTION] Tables inconnues ou absentes de la configuration: {sorted(unknown)}"
        )

    # Appel API direct (pas de XCom sensor)
    current_status = AMUEStatusChecker(AMUEAPIHook()).get_current_status()

    # AMUETableFilter reçoit uniquement les tables sélectionnées : validation ciblée
    # _last_report_start reste '' → import toujours FULL (comportement voulu en correction)
    table_filter = AMUETableFilter(tables_config=filtered_config)
    tables = table_filter.filter_tables(current_status)

    target_schema = bluegreen_ctx.get("target_schema") if bluegreen_ctx.get("enabled") else None
    for t in tables:
        t["target_schema"] = target_schema

    logger.info(f"[CORRECTION] {len(tables)} table(s) prête(s): {[t['table_name'] for t in tables]}")
    return tables
