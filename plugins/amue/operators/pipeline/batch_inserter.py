# amue/operators/pipeline/batch_inserter.py
"""
Execution des insertions SQL par batch.

Ce module gere l'insertion des donnees en base PostgreSQL par batch,
avec gestion des transactions et detection des conflits.
"""
import logging
import time
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from psycopg2 import sql, OperationalError, InterfaceError, ProgrammingError
from psycopg2.errors import UniqueViolation
from psycopg2.extras import execute_values

from amue.operators.pipeline.duplicate_detector import DuplicateDetector
from amue.exceptions import (
    AMUEBatchError,
    AMUEDatabaseError,
    AMUEDataError,
)
from amue.utils.config.settings import Defaults
from amue.utils.database.schema_utils import SchemaQualifier

logger = logging.getLogger(__name__)


class AMUEBatchInserter:
    """
    Execute les insertions SQL par batch avec gestion des erreurs.

    Mode UPSERT uniquement (INSERT ON CONFLICT DO UPDATE) pour garantir
    qu'aucune donnée n'est supprimée.

    Attributes:
        postgres_hook: Hook de connexion PostgreSQL
        duplicate_detector: Detecteur de doublons
        target_schema: Schéma cible pour blue/green (optionnel)

    Example:
        >>> inserter = AMUEBatchInserter(postgres_hook)
        >>> inserter.execute_batch(cursor, conn, sql, batch, table_name, columns, pks)
        >>> # Avec blue/green
        >>> inserter = AMUEBatchInserter(postgres_hook, target_schema='splus_blue')
    """

    def __init__(self, postgres_hook: Any = None, target_schema: Optional[str] = None):
        """
        Initialise l'inserter de batch.

        Args:
            postgres_hook: Hook PostgreSQL (optionnel, peut etre fourni plus tard)
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue')
        """
        self.postgres_hook = postgres_hook
        self.duplicate_detector = DuplicateDetector()
        self._schema_qualifier = SchemaQualifier(target_schema)
        self._conn: Optional[Any] = None

    @property
    def target_schema(self) -> Optional[str]:
        """Retourne le schéma cible."""
        return self._schema_qualifier.target_schema

    @target_schema.setter
    def target_schema(self, value: Optional[str]) -> None:
        """Définit le schéma cible."""
        self._schema_qualifier.target_schema = value

    def _get_qualified_table_name(self, table_name: str) -> str:
        """
        Retourne le nom de table qualifié avec le schéma.

        Args:
            table_name: Nom de la table

        Returns:
            Nom qualifié (ex: 'splus_blue.csks' ou 'csks' si pas de target_schema)
        """
        return self._schema_qualifier.qualify(table_name)

    def get_connection(self) -> Any:
        """Retourne une connexion reutilisable."""
        if self._conn is None or getattr(self._conn, 'closed', True):
            if self.postgres_hook is None:
                raise AirflowException("PostgreSQL hook non configure")
            self._conn = self.postgres_hook.get_conn()
        return self._conn

    def close_connection(self) -> None:
        """Ferme proprement la connexion"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def execute_batch(
        self,
        cursor,
        conn,
        insert_sql: str,
        batch: List[tuple],
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        commit: bool = True,
        batch_num: Optional[int] = None
    ) -> Dict[str, Any]:
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
            batch_num: Numéro du batch (pour le logging et les erreurs)

        Returns:
            Métriques du batch :
            {
                'rows_affected': int,   # rows_inserted + rows_updated
                'rows_inserted': int,   # nouvelles lignes (INSERT)
                'rows_updated': int,    # lignes mises à jour (UPDATE)
                'batch_size': int,
                'duration_seconds': float,
                'batch_num': int or None
            }

        Raises:
            AMUEDataError: En cas de doublons dans les données source
            AMUEBatchError: En cas de conflit de clé primaire
            AMUEDatabaseError: En cas d'erreur de connexion DB
        """
        start_time = time.monotonic()

        # Detection proactive des doublons AVANT insertion
        if primary_keys:
            duplicates_found = self.duplicate_detector.detect_duplicates_in_batch(
                batch, columns, primary_keys
            )
            if duplicates_found:
                self.duplicate_detector.log_batch_duplicates(
                    table_name, columns, primary_keys, duplicates_found
                )
                raise AMUEDataError(
                    f"Doublons detectes dans les donnees API pour {table_name}. "
                    f"{len(duplicates_found)} groupe(s) de doublons trouve(s). "
                    f"Voir les logs pour les details.",
                    table_name=table_name,
                    rows_imported=0
                )

        try:
            if primary_keys:
                # UPSERT avec RETURNING (xmax = 0) AS is_insert
                # xmax=0 → INSERT (nouvelle ligne), xmax≠0 → UPDATE (mise à jour)
                results = execute_values(cursor, insert_sql, batch,
                                         page_size=len(batch), fetch=True)
                rows_inserted = sum(1 for row in results if row[0])
                rows_updated = sum(1 for row in results if not row[0])
            else:
                # INSERT simple sans RETURNING : toutes les lignes sont nouvelles
                execute_values(cursor, insert_sql, batch, page_size=len(batch))
                rows_inserted = len(batch)
                rows_updated = 0

            if commit:
                conn.commit()

            duration = time.monotonic() - start_time
            return {
                'rows_affected': rows_inserted + rows_updated,
                'rows_inserted': rows_inserted,
                'rows_updated': rows_updated,
                'batch_size': len(batch),
                'duration_seconds': round(duration, 3),
                'batch_num': batch_num
            }

        except UniqueViolation as e:
            self._handle_unique_violation(
                e, cursor, conn, batch, table_name, columns, primary_keys, commit, batch_num
            )

        except ProgrammingError as e:
            if commit:
                try:
                    conn.rollback()
                except Exception as rollback_err:
                    logger.warning(f"{Defaults.LOG_PREFIX_BATCH} Rollback échoué: {rollback_err}")
            # Diagnostic spécifique : ON CONFLICT sans contrainte UNIQUE (pgcode 42P10)
            if getattr(e, 'pgcode', None) == '42P10' or 'on conflict' in str(e).lower():
                self._log_on_conflict_details(table_name, batch, columns, primary_keys, batch_num, conn)
            raise

        except (OperationalError, InterfaceError) as e:
            # Erreurs de connexion DB
            if commit:
                try:
                    conn.rollback()
                except Exception as rollback_err:
                    logger.warning(f"{Defaults.LOG_PREFIX_BATCH} Rollback échoué après erreur connexion: {rollback_err}")
            logger.error(f"{Defaults.LOG_PREFIX_BATCH} Erreur connexion DB: {e}")
            raise AMUEDatabaseError(
                f"Erreur de connexion lors de l'insertion du batch: {e}",
                table_name=table_name,
                is_connection_error=True
            ) from e

    def build_insert_sql(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        use_upsert: bool,
        conn,
        protected_source: Optional[str] = None
    ) -> str:
        """
        Construit la requete SQL d'insertion avec identifiants securises.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            use_upsert: True pour UPSERT, False pour INSERT simple
            conn: Connexion pour convertir la requete en string
            protected_source: Si fourni, les lignes ayant _source=protected_source
                              ne seront pas écrasées lors d'un conflit

        Returns:
            Requete SQL sous forme de string
        """
        # Construit l'identifiant de table (avec ou sans schéma)
        table_id = self._schema_qualifier.qualify_identifier(table_name)

        column_ids = [sql.Identifier(col) for col in columns]

        placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        column_list = sql.SQL(', ').join(column_ids)

        if use_upsert and primary_keys:
            pk_ids = [sql.Identifier(pk) for pk in primary_keys]
            # Colonnes à mettre à jour (exclut PKs et _source qui est préservé)
            update_cols = [
                sql.Identifier(col)
                for col in columns
                if col not in primary_keys and col != '_source'
            ]

            if protected_source:
                where_clause = sql.SQL(" WHERE {table}.{col} != {protected}").format(
                    table=table_id,
                    col=sql.Identifier('_source'),
                    protected=sql.Literal(protected_source)
                )
            else:
                where_clause = sql.SQL("")

            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders}) ON CONFLICT ({pks})
                DO UPDATE SET {updates}{where}
            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders,
                pks=sql.SQL(', ').join(pk_ids),
                updates=sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(col, col)
                    for col in update_cols
                ]),
                where=where_clause
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

    def build_insert_sql_for_values(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        use_upsert: bool,
        conn,
        protected_source: Optional[str] = None
    ) -> str:
        """
        Construit la requête SQL pour execute_values() (avec VALUES %s).

        Même logique que build_insert_sql() mais adaptée à execute_values()
        qui gère les placeholders automatiquement.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des clés primaires
            use_upsert: True pour UPSERT, False pour INSERT simple
            conn: Connexion pour convertir la requête en string
            protected_source: Si fourni, les lignes ayant _source=protected_source
                              ne seront pas écrasées lors d'un conflit.
                              rows_skipped = batch_size - rows_affected.

        Returns:
            Requête SQL avec VALUES %s
        """
        table_id = self._schema_qualifier.qualify_identifier(table_name)
        column_ids = [sql.Identifier(col) for col in columns]
        column_list = sql.SQL(', ').join(column_ids)

        if use_upsert and primary_keys:
            pk_ids = [sql.Identifier(pk) for pk in primary_keys]
            update_cols = [
                sql.Identifier(col)
                for col in columns
                if col not in primary_keys and col != '_source'
            ]

            if protected_source:
                where_clause = sql.SQL(" WHERE {table}.{col} != {protected}").format(
                    table=table_id,
                    col=sql.Identifier('_source'),
                    protected=sql.Literal(protected_source)
                )
            else:
                where_clause = sql.SQL("")

            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES %s ON CONFLICT ({pks})
                DO UPDATE SET {updates}{where}
                RETURNING (xmax = 0) AS is_insert
            """).format(
                table=table_id,
                columns=column_list,
                pks=sql.SQL(', ').join(pk_ids),
                updates=sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(col, col)
                    for col in update_cols
                ]),
                where=where_clause
            )
        else:
            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES %s
            """).format(
                table=table_id,
                columns=column_list
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
            Dict de la ligne existante ou None si non trouvée

        Raises:
            AMUEDatabaseError: En cas d'erreur de connexion à la base de données
        """
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
            logger.debug(f"{Defaults.LOG_PREFIX_BATCH} Pas de clause WHERE, retour None")
            return None

        # Utilise le nom qualifié si blue/green actif
        qualified_name = self._get_qualified_table_name(table_name)

        select_sql = f"""
            SELECT {', '.join(columns)}
            FROM {qualified_name}
            WHERE {' AND '.join(where_clauses)}
        """

        try:
            cursor.execute(select_sql, params)
            row = cursor.fetchone()

            if row:
                return dict(zip(columns, row))

            return None

        except (OperationalError, InterfaceError) as e:
            # Erreurs de connexion : doivent être remontées
            logger.error(f"{Defaults.LOG_PREFIX_BATCH} Erreur connexion DB: {e}")
            raise AMUEDatabaseError(
                f"Erreur de connexion lors de la récupération de la ligne existante: {e}",
                table_name=table_name,
                is_connection_error=True
            ) from e

        except Exception as e:
            # Autres erreurs (ex: table n'existe pas) : log et retourne None
            # Ces erreurs ne sont pas des erreurs de connexion
            logger.warning(f"{Defaults.LOG_PREFIX_BATCH} Erreur recuperation ligne: {e}")
            return None

    def _log_on_conflict_details(
        self,
        table_name: str,
        batch: List[tuple],
        columns: List[str],
        primary_keys: List[str],
        batch_num: Optional[int],
        conn=None,
    ) -> None:
        """
        Logge un comparatif existant-DB vs données-API pour chaque ligne du batch.

        Pour chaque ligne (5 max) : récupère la ligne existante en base via les valeurs PK,
        puis affiche colonne par colonne les différences (MODIFIÉ) et les valeurs inchangées.
        Si la ligne n'existe pas en base, affiche les données API telles quelles.
        """
        label = f"batch#{batch_num}" if batch_num is not None else "batch"
        data_cols = [c for c in columns if c not in primary_keys and c not in ('_source', '_imported_at')]

        logger.error(
            f"{Defaults.LOG_PREFIX_BATCH} [{label}] {table_name} : "
            f"pas de contrainte UNIQUE/PRIMARY KEY sur {primary_keys} dans PostgreSQL"
        )
        logger.error(
            f"{Defaults.LOG_PREFIX_BATCH}   {len(batch)} ligne(s) dans le batch — "
            f"comparatif base existante vs données API (5 premières) :"
        )
        sample = batch[:5]
        for i, row in enumerate(sample, 1):
            row_dict = dict(zip(columns, row))
            pk_vals = {pk: row_dict.get(pk, '?') for pk in primary_keys}
            logger.error(f"{Defaults.LOG_PREFIX_BATCH}   ── [{i}/{len(batch)}] PK = {pk_vals}")

            existing = None
            if conn is not None:
                try:
                    with conn.cursor() as cur:
                        existing = self.fetch_existing_row(
                            cur, conn, table_name, columns, primary_keys, pk_vals
                        )
                except Exception as fetch_err:
                    logger.debug(f"{Defaults.LOG_PREFIX_BATCH}     (fetch existant impossible: {fetch_err})")

            if existing is not None:
                changed = [
                    (col, existing.get(col), row_dict.get(col))
                    for col in data_cols
                    if existing.get(col) != row_dict.get(col)
                ]
                unchanged_count = len(data_cols) - len(changed)
                if changed:
                    logger.error(f"{Defaults.LOG_PREFIX_BATCH}     {len(changed)} colonne(s) MODIFIÉE(S) :")
                    for col, old_val, new_val in changed:
                        logger.error(
                            f"{Defaults.LOG_PREFIX_BATCH}       {col:<25} : {old_val!r:>30}  →  {new_val!r}"
                        )
                if unchanged_count:
                    logger.error(f"{Defaults.LOG_PREFIX_BATCH}     {unchanged_count} colonne(s) inchangée(s)")
            else:
                logger.error(f"{Defaults.LOG_PREFIX_BATCH}     → Absent en base — données API :")
                for col in data_cols:
                    logger.error(f"{Defaults.LOG_PREFIX_BATCH}       {col:<25} : {row_dict.get(col)!r}")

        if len(batch) > 5:
            logger.error(
                f"{Defaults.LOG_PREFIX_BATCH}   ... {len(batch) - 5} ligne(s) supplémentaire(s) non affichées"
            )

    def _handle_unique_violation(
        self,
        error: UniqueViolation,
        cursor,
        conn,
        batch: List[tuple],
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        commit: bool,
        batch_num: Optional[int] = None
    ) -> None:
        """
        Gere une erreur de violation de cle unique.

        Args:
            error: L'exception UniqueViolation
            cursor: Curseur de la connexion
            conn: Connexion
            batch: Le batch en erreur
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des clés primaires
            commit: Si le commit était prévu
            batch_num: Numéro du batch

        Raises:
            AMUEBatchError: Toujours levée après traitement
        """
        if commit:
            try:
                conn.rollback()
            except Exception as rollback_err:
                logger.warning(f"{Defaults.LOG_PREFIX_BATCH} Rollback échoué après UniqueViolation: {rollback_err}")

        logger.error(f"{Defaults.LOG_PREFIX_BATCH} Erreur de cle primaire dupliquee sur {table_name}")
        logger.error(f"{Defaults.LOG_PREFIX_BATCH} Message: {error.pgerror}")

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
                try:
                    existing_row = self.fetch_existing_row(
                        cursor, conn, table_name, columns, primary_keys, pk_values
                    )
                    conflicting_row = duplicates_in_batch[0] if duplicates_in_batch else None

                    self.duplicate_detector.log_conflict_details(
                        table_name, columns, primary_keys,
                        existing_row, conflicting_row, pk_values
                    )
                except AMUEDatabaseError:
                    # Si on ne peut pas récupérer la ligne existante, on continue
                    logger.warning(f"{Defaults.LOG_PREFIX_BATCH} Impossible de récupérer la ligne existante")

        raise AMUEBatchError(
            f"Conflit de cle primaire sur {table_name}. "
            f"Voir les logs pour les details des lignes en conflit.",
            table_name=table_name,
            batch_num=batch_num,
            batch_size=len(batch)
        )
