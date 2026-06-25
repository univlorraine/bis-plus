"""Task de découverte des tables exposées par le statut API AMUE."""
import logging
from typing import Dict, List

from airflow.sdk import task
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

from amue.application.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)

# Variable Airflow alimentant le dropdown du Param 'tables_to_add' du DAG
# (relue au prochain parsing du fichier DAG, cf. dags/dag_amue_table_discovery.py)
DISCOVERED_TABLES_VARIABLE = 'amue_discovered_new_tables'


@task(task_id='discover_tables')
def discover_tables() -> Dict[str, List[str]]:
    """
    Compare les tables vues dans le statut API AMUE à splus_admin.amue_tables.

    Sauvegarde la liste des tables nouvelles dans la Variable Airflow
    DISCOVERED_TABLES_VARIABLE pour alimenter le dropdown du Param
    'tables_to_add' lors du prochain parsing du DAG.

    Returns:
        Dict avec 'available' (toutes les tables vues côté API),
        'new' (absentes de la BDD) et 'known' (déjà enregistrées).
    """
    from amue.infrastructure.hooks.amue_api_hook import AMUEAPIHook
    from amue.application.api_source_factory import get_status_checker

    checker = get_status_checker(AMUEAPIHook())
    tables_status = checker.get_current_status()
    available = sorted(tables_status.keys())

    known = {t['table_name'].upper() for t in TableConfigManager().get_tables_config()}
    new = [name for name in available if name not in known]

    logger.info(
        f"[DISCOVERY] {len(available)} table(s) exposée(s) par l'API, "
        f"{len(new)} nouvelle(s) non enregistrée(s): {new}"
    )

    VarMgr.set(DISCOVERED_TABLES_VARIABLE, new)

    return {'available': available, 'new': new, 'known': sorted(known)}
