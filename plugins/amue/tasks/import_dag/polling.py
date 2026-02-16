"""Task de polling API et sélection des tables."""
import json
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.services.api.status_checker import AMUEStatusChecker
from amue.services.api.polling_service import AMUEPollingService
from amue.operators.table_management.table_filter import AMUETableFilter
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@task(task_id='wait_for_api_and_select')
def wait_for_api_and_select(bluegreen_ctx: Dict) -> List[Dict]:
    """
    Attend la disponibilité de l'API et sélectionne les tables à importer.

    Étapes :
        1. Polling de l'API jusqu'à disponibilité (code HTTP 200)
        2. Vérification de la variable 'finish' (traitement AMUE terminé)
        3. Récupération du statut actuel de toutes les tables
        4. Filtrage selon la configuration (amue_tables_to_import)

    Returns:
        Liste de dictionnaires contenant les infos de chaque table.

    Raises:
        AirflowException: Si l'API n'est pas disponible après le timeout
    """
    # Initialisation des services
    api_hook = AMUEAPIHook()
    status_checker = AMUEStatusChecker(api_hook)
    polling_service = AMUEPollingService(status_checker)

    # --- Étape 1 : Attente de l'API ---
    logger.info("[INIT] Attente disponibilité API...")
    polling_result = polling_service.wait_for_ready()

    # Stocke les infos de polling pour le rapport final
    VarMgr.set('_current_run_polling', json.dumps(polling_result, default=str))

    # --- Étape 2 : Sélection des tables ---
    logger.info("[INIT] API prête, sélection des tables...")

    current_status = polling_result.get('tables_status', {})
    if not current_status:
        logger.warning("[INIT] tables_status non disponible, appel API de secours")
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
