"""Task de sélection de tables pour un run de correction ECC."""
import logging
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context, task

from common.application.bluegreen.bluegreen_manager import BlueGreenManager
from common.infrastructure.database.hooks import create_postgres_hook
from ecc.infrastructure.config.settings import ECCDefaults

logger = logging.getLogger(__name__)


@task(task_id='select_ecc_tables_correction')
def select_tables_correction() -> List[Dict]:
    """
    Sélection de tables ECC pour un run de correction manuelle.

    Lit la liste depuis dag_run.conf['selected_tables'] (obligatoire).
    Même logique que select_tables() mais limitée aux tables sélectionnées.
    Cible uniquement le schéma inactif (cohérent avec le comportement normal ECC).

    Returns:
        Liste de dicts par table × schéma sélectionnés.
    """
    context = get_current_context()
    dag_run = context.get('dag_run')
    selected_tables = list((dag_run.conf or {}).get('selected_tables', [])) if dag_run else []

    if not selected_tables:
        raise AirflowException(
            "[CORRECTION] selected_tables est requis. "
            "Déclenchez via 'Trigger DAG w/ config' et sélectionnez les tables cibles."
        )

    logger.info(f"[CORRECTION] Tables ECC demandées: {selected_tables}")

    manager = BlueGreenManager()
    inactive_canonical = manager.get_target_schema()
    inactive_offline = inactive_canonical + BlueGreenManager.OFFLINE_SUFFIX

    if manager.schema_exists(inactive_canonical):
        inactive = inactive_canonical
    elif manager.schema_exists(inactive_offline):
        inactive = inactive_offline
    else:
        inactive = None

    if inactive:
        schemas = [inactive]
        logger.info(f"[CORRECTION] Schéma cible inactif: {inactive}")
    else:
        active = manager.get_active_schema()
        schemas = [s for s in [active, inactive] if s]
        logger.warning(f"[CORRECTION] Pas d'inactif — import dans: {schemas}")

    # Charge toutes les tables ECC et filtre sur la sélection
    selected_lower = {t.lower() for t in selected_tables}
    pg_hook = create_postgres_hook(schema='splus_admin')
    all_rows = pg_hook.get_records(
        """
        SELECT table_name, ecc_query, primary_key
        FROM splus_admin.amue_tables
        WHERE enabled = TRUE
          AND ecc_query IS NOT NULL
          AND ecc_query != ''
        ORDER BY table_name
        """
    )

    rows = [(n, q, pk) for n, q, pk in all_rows if n.lower() in selected_lower]
    unknown = selected_lower - {n.lower() for n, _, _ in rows}
    if unknown:
        raise AirflowException(
            f"[CORRECTION] Tables inconnues ou sans ecc_query configurée: {sorted(unknown)}"
        )

    result = []
    for schema in schemas:
        for table_name, ecc_query, primary_key in rows:
            pk_list = [pk.strip() for pk in primary_key.split(',') if pk.strip()]
            result.append({
                'table_name': table_name,
                'ecc_query': ecc_query,
                'primary_keys': pk_list,
                'target_schema': schema,
                'source': ECCDefaults.SOURCE_NAME,
                'protected_source': ECCDefaults.PROTECTED_SOURCE,
            })
            logger.debug(f"[CORRECTION] Table sélectionnée: {table_name} → {schema}")

    logger.info(f"[CORRECTION] {len(result)} entrée(s) ({len(rows)} table(s) × {len(schemas)} schéma(s))")
    return result
