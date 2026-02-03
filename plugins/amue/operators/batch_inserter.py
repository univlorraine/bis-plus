# amue/operators/batch_inserter.py
"""
Execution des insertions SQL par batch.

Ce module gere l'insertion des donnees en base PostgreSQL par batch,
avec gestion des transactions et detection des conflits.
"""
import logging
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from psycopg2 import sql
from psycopg2.errors import UniqueViolation

from amue.operators.duplicate_detector import DuplicateDetector

logger = logging.getLogger(__name__)


class AMUEBatchInserter:
    """
    Execute les insertions SQL par batch avec gestion des erreurs.

    Supporte deux modes:
    - INSERT simple (pour import FULL avec TRUNCATE prealable)
    - UPSERT (INSERT ON CONFLICT DO UPDATE pour import differentiel)

    Attributes:
        postgres_hook: Hook de connexion PostgreSQL
        duplicate_detector: Detecteur de doublons

    Example:
        >>> inserter = AMUEBatchInserter(postgres_hook)
        >>> inserter.execute_batch(cursor, conn, sql, batch, table_name, columns, pks)
    """

    def __init__(self, postgres_hook=None):
        """
        Initialise l'inserter de batch.

        Args:
            postgres_hook: Hook PostgreSQL (optionnel, peut etre fourni plus tard)
        """
        self.postgres_hook = postgres_hook
        self.duplicate_detector = DuplicateDetector()
        self._conn = None

    def set_postgres_hook(self, postgres_hook) -> None:
        """Configure le hook PostgreSQL"""
        self.postgres_hook = postgres_hook

    def get_connection(self):
        """Retourne une connexion reutilisable"""
        if self._conn is None or self._conn.closed:
            if self.postgres_hook is None:
                raise AirflowException("PostgreSQL hook non configure")
            self._conn = self.postgres_hook.get_conn()
        return self._conn

    def close_connection(self) -> None:
        """Ferme proprement la connexion"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def truncate_table(self, cursor, table_name: str) -> None:
        """
        Vide la table (pour import FULL) - SANS COMMIT.

        Le commit sera fait a la fin de l'import complet pour garantir
        l'atomicite de l'operation (TRUNCATE + INSERT dans une seule transaction).

        Args:
            cursor: Curseur de la connexion
            table_name: Nom de la table a vider
        """
        truncate_sql = sql.SQL("TRUNCATE TABLE {table} CASCADE").format(
            table=sql.Identifier(table_name)
        )
        cursor.execute(truncate_sql)
        logger.info(f"[FULL IMPORT] TRUNCATE prepare pour {table_name} (en attente du commit final)")

    def execute_batch(
        self,
        cursor,
        conn,
        insert_sql: str,
        batch: List[tuple],
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        commit: bool = True
    ) -> None:
        """
        Execute un batch d'insertions avec detection des conflits.

        Args:
            cursor: Curseur de la connexion
            conn: Connexion a la base de donnees
            insert_sql: Requete SQL d'insertion
            batch: Liste des tuples a inserer
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            commit: Si True, commit apres l'insertion

        Raises:
            AirflowException: En cas de doublons ou conflit de cle primaire
        """
        # Detection proactive des doublons AVANT insertion
        if primary_keys:
            duplicates_found = self.duplicate_detector.detect_duplicates_in_batch(
                batch, columns, primary_keys
            )
            if duplicates_found:
                self.duplicate_detector.log_batch_duplicates(
                    table_name, columns, primary_keys, duplicates_found
                )
                raise AirflowException(
                    f"Doublons detectes dans les donnees API pour {table_name}. "
                    f"{len(duplicates_found)} groupe(s) de doublons trouve(s). "
                    f"Voir les logs pour les details."
                )

        try:
            cursor.executemany(insert_sql, batch)
            if commit:
                conn.commit()

        except UniqueViolation as e:
            self._handle_unique_violation(
                e, cursor, conn, batch, table_name, columns, primary_keys, commit
            )

    def build_insert_sql(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        use_upsert: bool,
        conn
    ) -> str:
        """
        Construit la requete SQL d'insertion avec identifiants securises.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            use_upsert: True pour UPSERT, False pour INSERT simple
            conn: Connexion pour convertir la requete en string

        Returns:
            Requete SQL sous forme de string
        """
        table_id = sql.Identifier(table_name)
        column_ids = [sql.Identifier(col) for col in columns]

        placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        column_list = sql.SQL(', ').join(column_ids)

        if use_upsert and primary_keys:
            pk_ids = [sql.Identifier(pk) for pk in primary_keys]
            update_cols = [sql.Identifier(col) for col in columns if col not in primary_keys]

            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders}) ON CONFLICT ({pks})
                DO UPDATE SET {updates}
            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders,
                pks=sql.SQL(', ').join(pk_ids),
                updates=sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(col, col)
                    for col in update_cols
                ])
            )
        else:
            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders
            )

        return query.as_string(conn)

    def fetch_existing_row(
        self,
        cursor,
        conn,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        pk_values: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """
        Recupere la ligne existante en base avec les valeurs de cle primaire.

        Args:
            cursor: Curseur de la connexion
            conn: Connexion
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            pk_values: Valeurs de la cle primaire

        Returns:
            Dict de la ligne existante ou None
        """
        try:
            where_clauses = []
            params = []

            for pk in primary_keys:
                pk_lower = pk.lower()
                if pk_lower in pk_values:
                    where_clauses.append(f"{pk_lower} = %s")
                    params.append(pk_values[pk_lower])
                elif pk in pk_values:
                    where_clauses.append(f"{pk.lower()} = %s")
                    params.append(pk_values[pk])

            if not where_clauses:
                return None

            select_sql = f"""
                SELECT {', '.join(columns)}
                FROM {table_name}
                WHERE {' AND '.join(where_clauses)}
            """

            cursor.execute(select_sql, params)
            row = cursor.fetchone()

            if row:
                return dict(zip(columns, row))

            return None

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur recuperation ligne existante: {e}")
            return None

    def _handle_unique_violation(
        self,
        error: UniqueViolation,
        cursor,
        conn,
        batch: List[tuple],
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        commit: bool
    ) -> None:
        """Gere une erreur de violation de cle unique"""
        if commit:
            conn.rollback()

        logger.error(f"[CONFLIT PK] Erreur de cle primaire dupliquee sur {table_name}")
        logger.error(f"[CONFLIT PK] Message: {error.pgerror}")

        pk_values = self.duplicate_detector.extract_pk_from_error(
            str(error.pgerror), primary_keys
        )

        if pk_values and primary_keys:
            duplicates_in_batch = self.duplicate_detector.find_duplicates_for_pk(
                batch, columns, primary_keys, pk_values
            )

            if len(duplicates_in_batch) > 1:
                self.duplicate_detector.log_api_duplicates(
                    table_name, columns, primary_keys, duplicates_in_batch, pk_values
                )
            else:
                existing_row = self.fetch_existing_row(
                    cursor, conn, table_name, columns, primary_keys, pk_values
                )
                conflicting_row = duplicates_in_batch[0] if duplicates_in_batch else None

                self.duplicate_detector.log_conflict_details(
                    table_name, columns, primary_keys,
                    existing_row, conflicting_row, pk_values
                )

        raise AirflowException(
            f"Conflit de cle primaire sur {table_name}. "
            f"Voir les logs pour les details des lignes en conflit."
        )
