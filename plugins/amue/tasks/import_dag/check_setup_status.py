"""Task de vérification du statut de setup avant import."""
import logging
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.sdk import task

from amue.services.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


@task(task_id='check_setup_status')
def check_setup_status(tables: List[Dict]) -> List[Dict]:
    """
    Vérifie que toutes les tables sont prêtes avant l'import.

    Cette task remplace verify_table + validate_tables + prepare_table.
    Elle ne fait aucun appel API : elle lit uniquement le setup_status
    depuis splus_admin.amue_tables (mis à jour par la DAG amue_table_setup).

    Comportement FAIL-FAST :
        - setup_status == 'pending'  → AirflowException (setup jamais exécuté)
        - setup_status == 'blocked'  → AirflowException (changement de structure détecté)
        - setup_status == 'ready'    → OK, table incluse dans la liste retournée

    Args:
        tables: Liste des tables à importer (format select_tables())

    Returns:
        Liste des tables prêtes, enrichies avec primary_key depuis la config DB

    Raises:
        AirflowException: Si une ou plusieurs tables ne sont pas prêtes
    """
    config_manager = TableConfigManager()
    errors = []
    ready = []

    for table in tables:
        table_name = table.get('table_name', 'unknown')
        metadata = config_manager.get_table_metadata(table_name)

        if metadata is None:
            errors.append(f"{table_name}: introuvable dans splus_admin.amue_tables")
            continue

        status = metadata.get('setup_status', 'pending')

        if status == 'pending':
            errors.append(
                f"{table_name}: setup_status='pending' — "
                f"lancer la DAG amue_table_setup avant l'import"
            )
        elif status == 'blocked':
            fp_api = metadata.get('fingerprint_API', 'N/A')
            fp_ul = metadata.get('fingerprint_UL', 'N/A')
            pk = metadata.get('primary_key', 'N/A')
            logger.error(f"[CHECK_SETUP] {table_name} : BLOQUÉE — changement de structure AMUE détecté")
            logger.error(f"[CHECK_SETUP]   fingerprint_API stocké : {fp_api} (dernière structure valide connue)")
            logger.error(f"[CHECK_SETUP]   fingerprint_UL stocké  : {fp_ul}")
            logger.error(f"[CHECK_SETUP]   primary_key configurée : {pk}")
            logger.error(f"[CHECK_SETUP]   → Relancer amue_table_setup pour comparer la structure actuelle")
            errors.append(
                f"{table_name}: setup_status='blocked' — "
                f"changement de structure détecté, intervention manuelle requise"
            )
        else:
            # Enrichit la table avec primary_key depuis la config DB
            enriched = dict(table)
            enriched['primary_key'] = metadata.get('primary_key', table.get('primary_key', ''))
            ready.append(enriched)
            logger.info(f"[CHECK_SETUP] {table_name}: prête pour l'import")

    if errors:
        detail = '\n'.join(f"  - {e}" for e in errors)
        raise AirflowException(
            f"{len(errors)} table(s) non prête(s) pour l'import :\n{detail}"
        )

    logger.info(f"[CHECK_SETUP] {len(ready)} table(s) validée(s)")
    return ready
