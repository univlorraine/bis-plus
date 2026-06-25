"""Task d'enregistrement des tables sélectionnées dans splus_admin.amue_tables."""
import logging
from typing import Dict, List

from airflow.sdk import task

from amue.application.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


@task(task_id='register_tables')
def register_tables(discovery: Dict[str, List[str]], **context) -> List[str]:
    """
    Enregistre les nouvelles tables sélectionnées via les Params du DAG.

    Args:
        discovery: Résultat de discover_tables() ({'available', 'new', 'known'})

    Params Airflow consommés (context['params']) :
        add_all_discovered : si True, enregistre toutes les tables 'new'
        tables_to_add       : sinon, liste explicite de noms à enregistrer
        enabled_default     : valeur de la colonne enabled pour les nouvelles entrées

    Returns:
        Liste des noms de tables effectivement enregistrés
    """
    params = context['params']
    new_tables = discovery.get('new', [])

    if params.get('add_all_discovered', False):
        targets = new_tables
    else:
        wanted = {t.strip().upper() for t in params.get('tables_to_add', []) if t and t.strip()}
        targets = [t for t in new_tables if t in wanted]
        unknown = wanted - set(new_tables)
        if unknown:
            logger.warning(
                f"[DISCOVERY] Ignoré(s) — absent(s) du statut API ou déjà enregistré(s): {sorted(unknown)}"
            )

    if not targets:
        logger.info("[DISCOVERY] Aucune table à enregistrer pour ce run")
        return []

    added = TableConfigManager().register_tables(targets, enabled=params.get('enabled_default', True))
    return added
