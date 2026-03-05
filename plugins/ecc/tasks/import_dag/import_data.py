# ecc/tasks/import_dag/import_data.py
"""Task d'import Oracle → PostgreSQL pour une table ECC."""
import logging
from datetime import datetime
from typing import Dict, List

from airflow.sdk import task

from amue.operators.pipeline.batch_inserter import AMUEBatchInserter
from amue.operators.table_management.table_manager import AMUETableManager
from amue.utils.database.hooks import create_postgres_hook
from ecc.hooks.ecc_source_hook import ECCSourceHook
from ecc.utils.config.settings import get_ecc_batch_size

logger = logging.getLogger(__name__)


def _check_table_exists(pg_hook, table_name: str, schema_name: str) -> bool:
    """Vérifie si une table existe dans un schéma PostgreSQL."""
    result = pg_hook.get_first(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        parameters=(schema_name, table_name.lower())
    )
    return bool(result[0]) if result else False


@task(task_id='import_ecc_data')
def import_ecc_data(table_config: Dict) -> Dict:
    """
    Importe une table Oracle SAP ECC vers PostgreSQL via UPSERT.

    Protection sifac_plus : si la ligne en conflit a _source='sifac_plus',
    elle n'est pas remplacée (guard WHERE dans le DO UPDATE SET).

    Args:
        table_config: Config table depuis select_ecc_tables() :
            {table_name, ecc_query, primary_keys (list), target_schema,
             source, protected_source}

    Returns:
        {table_name, rows_fetched, rows_inserted, rows_updated, rows_skipped,
         status, target_schema}
    """
    table_name = table_config['table_name']
    ecc_query = table_config['ecc_query']
    primary_keys: List[str] = table_config['primary_keys']
    target_schema = table_config['target_schema']
    source = table_config.get('source', 'ecc')
    protected_source = table_config.get('protected_source', 'sifac_plus')
    batch_size = get_ecc_batch_size()

    logger.info(f"[ECC] Import table: {table_name} → {target_schema}")

    # ── 1. Récupération Oracle ────────────────────────────────────────────────
    ecc_hook = ECCSourceHook()
    column_names, row_gen = ecc_hook.execute_query(ecc_query, batch_size=batch_size)

    # Colonnes complètes (Oracle + méta)
    all_columns = column_names + ['_source', '_imported_at']

    # ── 2. Vérification / création de la table PostgreSQL ────────────────────
    pg_hook = create_postgres_hook(bluegreen_schema=target_schema)
    table_exists = _check_table_exists(pg_hook, table_name, target_schema)

    manager = AMUETableManager(target_schema=target_schema)
    structure_info = {
        'table_name': table_name,
        'columns': [{'name': col, 'type_postgres': 'TEXT'} for col in column_names],
        'primary_keys': ','.join(primary_keys),
        'exists': table_exists,
    }
    manager.manage_table(structure_info)

    # ── 3. Construction du SQL UPSERT avec protection sifac_plus ─────────────
    inserter = AMUEBatchInserter(postgres_hook=pg_hook, target_schema=target_schema)
    conn = inserter.get_connection()
    cursor = conn.cursor()

    insert_sql = inserter.build_insert_sql_for_values(
        table_name, all_columns, primary_keys,
        use_upsert=bool(primary_keys),
        conn=conn,
        protected_source=protected_source
    )

    # ── 4. Stream Oracle → batch → PostgreSQL ────────────────────────────────
    batch = []
    rows_fetched = 0
    rows_inserted = 0
    rows_updated = 0
    rows_skipped = 0
    now = datetime.now()

    try:
        for row in row_gen:
            row_with_meta = tuple(row) + (source, now)
            batch.append(row_with_meta)
            rows_fetched += 1

            if len(batch) >= batch_size:
                result = inserter.execute_batch(
                    cursor, conn, insert_sql, batch,
                    table_name, all_columns, primary_keys,
                    commit=True
                )
                rows_inserted += result['rows_inserted']
                rows_updated += result['rows_updated']
                rows_skipped += result['batch_size'] - result['rows_affected']
                batch = []
                logger.info(
                    f"[ECC] {table_name}: {rows_fetched} lignes traitées "
                    f"(+{result['rows_inserted']} insérées, "
                    f"~{result['batch_size'] - result['rows_affected']} protégées)"
                )

        # Batch final
        if batch:
            result = inserter.execute_batch(
                cursor, conn, insert_sql, batch,
                table_name, all_columns, primary_keys,
                commit=True
            )
            rows_inserted += result['rows_inserted']
            rows_updated += result['rows_updated']
            rows_skipped += result['batch_size'] - result['rows_affected']

    finally:
        inserter.close_connection()

    logger.info(
        f"[ECC] {table_name} terminé: {rows_fetched} récupérées, "
        f"{rows_inserted} insérées, {rows_updated} mises à jour, "
        f"{rows_skipped} protégées (sifac_plus)"
    )

    return {
        'table_name': table_name,
        'rows_fetched': rows_fetched,
        'rows_inserted': rows_inserted,
        'rows_updated': rows_updated,
        'rows_skipped': rows_skipped,
        'status': 'success',
        'target_schema': target_schema,
        'import_type': 'full',
    }
