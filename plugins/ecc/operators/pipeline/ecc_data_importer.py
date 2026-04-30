"""
Import Oracle SAP ECC → PostgreSQL.

Encapsule la logique d'import d'une table ECC : création de la table cible
si absente, streaming Oracle → PostgreSQL en batches, UPSERT avec protection
sifac_plus.
"""
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

from common.log_prefixes import LogPrefixes
from common.operators.batch_inserter import BatchInserter
from common.utils.database.hooks import create_postgres_hook
from ecc.hooks.ecc_source_hook import ECCSourceHook

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


class ECCDataImporter:
    """
    Importe une table Oracle ECC vers PostgreSQL.

    Crée la table cible (en TEXT) si elle n'existe pas, puis streame les
    lignes Oracle par batches et exécute un UPSERT avec protection
    `_source='sifac_plus'` (les lignes sifac_plus ne sont jamais écrasées).
    """

    def __init__(self, target_schema: str, source: str, protected_source: str):
        self.target_schema = target_schema
        self.source = source
        self.protected_source = protected_source
        self.pg_hook = create_postgres_hook(bluegreen_schema=target_schema)

    def import_table(
        self,
        table_name: str,
        ecc_query: str,
        primary_keys: List[str],
        batch_size: int,
    ) -> Dict:
        """
        Exécute l'import d'une table ECC.

        Returns:
            {table_name, rows_fetched, rows_inserted, rows_updated, rows_skipped,
             status, target_schema, import_type}
        """
        logger.info(f"{LogPrefixes.ECC_IMPORT} Import table: {table_name} → {self.target_schema}")

        ecc_hook = ECCSourceHook()
        column_names, row_gen = ecc_hook.execute_query(ecc_query, batch_size=batch_size)
        all_columns = column_names + ['_source', '_imported_at']

        self._ensure_table_exists(table_name, column_names, primary_keys)

        inserter = BatchInserter(postgres_hook=self.pg_hook, target_schema=self.target_schema)
        conn = inserter.get_connection()
        cursor = conn.cursor()

        insert_sql = inserter.build_insert_sql_for_values(
            table_name, all_columns, primary_keys,
            use_upsert=bool(primary_keys),
            conn=conn,
            protected_source=self.protected_source,
        )

        rows_fetched = 0
        rows_inserted = 0
        rows_updated = 0
        rows_skipped = 0
        now = datetime.now()
        batch: List[tuple] = []

        try:
            for row in row_gen:
                batch.append(tuple(row) + (self.source, now))
                rows_fetched += 1

                if len(batch) >= batch_size:
                    counts = self._flush_batch(
                        inserter, cursor, conn, insert_sql, batch,
                        table_name, all_columns, primary_keys,
                    )
                    rows_inserted += counts['rows_inserted']
                    rows_updated += counts['rows_updated']
                    rows_skipped += counts['rows_skipped']
                    logger.info(
                        f"{LogPrefixes.ECC_IMPORT} {table_name}: {rows_fetched} lignes traitées "
                        f"(+{counts['rows_inserted']} insérées, ~{counts['rows_skipped']} protégées)"
                    )
                    batch = []

            if batch:
                counts = self._flush_batch(
                    inserter, cursor, conn, insert_sql, batch,
                    table_name, all_columns, primary_keys,
                )
                rows_inserted += counts['rows_inserted']
                rows_updated += counts['rows_updated']
                rows_skipped += counts['rows_skipped']
        finally:
            inserter.close_connection()

        logger.info(
            f"{LogPrefixes.ECC_IMPORT} {table_name} terminé: {rows_fetched} récupérées, "
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
            'target_schema': self.target_schema,
            'import_type': 'full',
        }

    def _ensure_table_exists(
        self,
        table_name: str,
        column_names: List[str],
        primary_keys: List[str],
    ) -> None:
        """Crée la table cible avec colonnes TEXT si elle n'existe pas."""
        if self._table_exists(table_name):
            return

        if not _SAFE_IDENTIFIER_RE.match(table_name):
            raise ValueError(f"Nom de table non sécurisé pour le DDL: {table_name!r}")
        for col in column_names:
            if not _SAFE_IDENTIFIER_RE.match(col):
                raise ValueError(f"Nom de colonne non sécurisé pour le DDL: {col!r}")

        col_defs = [f'"{col}" TEXT' for col in column_names]
        col_defs.append(f"_source VARCHAR(50) DEFAULT '{self.source}'")
        col_defs.append("_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        pk_clause = ''
        if primary_keys:
            for pk in primary_keys:
                if not _SAFE_IDENTIFIER_RE.match(pk):
                    raise ValueError(f"Clé primaire non sécurisée pour le DDL: {pk!r}")
            pk_cols = ', '.join(f'"{pk}"' for pk in primary_keys)
            pk_clause = f",\n    PRIMARY KEY ({pk_cols})"

        cols_sql = ',\n    '.join(col_defs)
        create_sql = (
            f'CREATE TABLE "{self.target_schema}"."{table_name}" (\n'
            f'    {cols_sql}{pk_clause}\n'
            f');'
        )
        logger.info(f"{LogPrefixes.ECC_IMPORT} Création table {self.target_schema}.{table_name}")
        self.pg_hook.run(create_sql)

    def _table_exists(self, table_name: str) -> bool:
        result = self.pg_hook.get_first(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            parameters=(self.target_schema, table_name.lower()),
        )
        return bool(result[0]) if result else False

    @staticmethod
    def _flush_batch(
        inserter: BatchInserter,
        cursor,
        conn,
        insert_sql: str,
        batch: List[tuple],
        table_name: str,
        all_columns: List[str],
        primary_keys: List[str],
    ) -> Dict[str, int]:
        result = inserter.execute_batch(
            cursor, conn, insert_sql, batch,
            table_name, all_columns, primary_keys,
            commit=True,
        )
        return {
            'rows_inserted': result['rows_inserted'],
            'rows_updated': result['rows_updated'],
            'rows_skipped': result['batch_size'] - result['rows_affected'],
        }
