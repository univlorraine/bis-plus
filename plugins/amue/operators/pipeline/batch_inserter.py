# amue/operators/pipeline/batch_inserter.py
"""
Execution des insertions SQL par batch.

Ce module gere l'insertion des donnees en base PostgreSQL par batch,
avec gestion des transactions et detection des conflits.
"""
import logging
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from psycopg2 import sql, OperationalError, InterfaceError
from psycopg2.errors import UniqueViolation

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
            batch_num: Numéro du batch (pour le logging et les erreurs)

        Raises:
            AMUEDataError: En cas de doublons dans les données source
            AMUEBatchError: En cas de conflit de clé primaire
            AMUEDatabaseError: En cas d'erreur de connexion DB
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
                raise AMUEDataError(
                    f"Doublons detectes dans les donnees API pour {table_name}. "
                    f"{len(duplicates_found)} groupe(s) de doublons trouve(s). "
                    f"Voir les logs pour les details.",
                    table_name=table_name,
                    rows_imported=0
                )

        try:
            cursor.executemany(insert_sql, batch)
            if commit:
                conn.commit()

        except UniqueViolation as e:
            self._handle_unique_violation(
                e, cursor, conn, batch, table_name, columns, primary_keys, commit, batch_num
            )

        except (OperationalError, InterfaceError) as e:
            # Erreurs de connexion DB
            if commit:
                try:
                    conn.rollback()
                except Exception:
                    pass  # Connexion peut être déjà perdue
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
            except Exception:
                pass  # Ignore si rollback échoue

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
