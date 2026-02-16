"""
Service de switch atomique des vues pour l'architecture Blue/Green.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module gère le basculement atomique des vues du schéma 'splus' vers
le schéma blue ou green. Toutes les vues sont recréées dans une seule
transaction pour garantir l'atomicité.

ARCHITECTURE DES VUES :
    splus.table_name -> CREATE VIEW AS SELECT * FROM splus_blue.table_name
                    OU
                     -> CREATE VIEW AS SELECT * FROM splus_green.table_name

================================================================================
ATOMICITÉ
================================================================================

Le switch est atomique grâce à :
    1. Toutes les vues sont modifiées dans une seule transaction
    2. En cas d'erreur, ROLLBACK complet
    3. Les utilisateurs voient soit l'ancien soit le nouveau schéma, jamais un état intermédiaire

================================================================================
"""
import logging
from typing import List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from amue.utils.database.hooks import create_postgres_hook
from amue.utils.database.connection_manager import PostgresConnectionManager

logger = logging.getLogger(__name__)


class ViewSwitcher:
    """
    Gère le switch atomique des vues vers un schéma cible.

    Cette classe est responsable de :
        - Lister toutes les tables d'un schéma
        - Recréer les vues dans splus pour pointer vers le schéma cible
        - Garantir l'atomicité du switch (tout ou rien)

    Example:
        >>> switcher = ViewSwitcher()
        >>> success = switcher.switch_views_to_schema('splus_green')
        >>> if success:
        ...     print("Vues basculées vers splus_green")
    """

    VIEW_SCHEMA = "splus"

    def __init__(self, postgres_hook: PostgresHook = None):
        """
        Initialise le ViewSwitcher.

        Args:
            postgres_hook: Hook PostgreSQL (créé si non fourni)
        """
        self.postgres_hook = postgres_hook or create_postgres_hook(schema='public')

    def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """
        Liste toutes les tables dans un schéma.

        Args:
            schema_name: Nom du schéma (ex: 'splus_blue')

        Returns:
            Liste des noms de tables
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        result = self.postgres_hook.get_records(query, parameters=(schema_name,))
        tables = [row[0] for row in result] if result else []
        logger.info(f"[VIEW_SWITCH] {len(tables)} tables trouvées dans {schema_name}")
        return tables

    def get_views_in_schema(self, schema_name: str) -> List[str]:
        """
        Liste toutes les vues dans un schéma.

        Args:
            schema_name: Nom du schéma (ex: 'splus')

        Returns:
            Liste des noms de vues
        """
        query = """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = %s
            ORDER BY table_name
        """
        result = self.postgres_hook.get_records(query, parameters=(schema_name,))
        views = [row[0] for row in result] if result else []
        logger.info(f"[VIEW_SWITCH] {len(views)} vues trouvées dans {schema_name}")
        return views

    def switch_views_to_schema(self, target_schema: str) -> bool:
        """
        Bascule toutes les vues vers le schéma cible.

        Cette opération est atomique : soit toutes les vues sont basculées,
        soit aucune en cas d'erreur.

        Args:
            target_schema: Schéma cible (ex: 'splus_blue' ou 'splus_green')

        Returns:
            True si le switch a réussi, False sinon

        Raises:
            Exception: En cas d'erreur SQL (rollback automatique)
        """
        logger.info(f"[VIEW_SWITCH] Switch des vues vers {target_schema}")

        # Récupère la liste des tables dans le schéma cible
        tables = self.get_tables_in_schema(target_schema)

        if not tables:
            logger.warning(f"[VIEW_SWITCH] Aucune table dans {target_schema}, abandon")
            return False

        with PostgresConnectionManager(self.postgres_hook) as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()

            try:
                # DROP puis CREATE chaque vue dans une transaction unique
                for table_name in tables:
                    drop_sql = sql.SQL(
                        "DROP VIEW IF EXISTS {view_schema}.{table}"
                    ).format(
                        view_schema=sql.Identifier(self.VIEW_SCHEMA),
                        table=sql.Identifier(table_name)
                    )
                    cursor.execute(drop_sql)

                    create_sql = sql.SQL(
                        "CREATE VIEW {view_schema}.{table} AS SELECT * FROM {target_schema}.{table}"
                    ).format(
                        view_schema=sql.Identifier(self.VIEW_SCHEMA),
                        target_schema=sql.Identifier(target_schema),
                        table=sql.Identifier(table_name)
                    )
                    cursor.execute(create_sql)
                    logger.debug(f"[VIEW_SWITCH] Vue {self.VIEW_SCHEMA}.{table_name} -> {target_schema}.{table_name}")

                # Commit atomique de toutes les vues
                conn_mgr.commit()
                logger.info(f"[VIEW_SWITCH] SUCCESS - {len(tables)} vues basculées vers {target_schema}")
                return True

            except Exception as e:
                conn_mgr.rollback()
                logger.error(f"[VIEW_SWITCH] ERREUR - Rollback: {e}")
                return False
            finally:
                cursor.close()

    def verify_views_point_to(self, expected_schema: str) -> bool:
        """
        Vérifie que toutes les vues pointent vers le schéma attendu.

        Args:
            expected_schema: Schéma attendu (ex: 'splus_blue')

        Returns:
            True si toutes les vues pointent vers le bon schéma
        """
        logger.info(f"[VIEW_SWITCH] Vérification des vues vers {expected_schema}")

        # Récupère la définition des vues
        query = """
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = %s
        """
        views = self.postgres_hook.get_records(query, parameters=(self.VIEW_SCHEMA,))

        if not views:
            logger.warning(f"[VIEW_SWITCH] Aucune vue dans {self.VIEW_SCHEMA}")
            return True  # Pas de vues = OK (peut être le premier run)

        all_correct = True
        for view_name, view_def in views:
            # Vérifie que la définition contient le bon schéma
            if expected_schema.lower() not in view_def.lower():
                logger.error(f"[VIEW_SWITCH] Vue {view_name} ne pointe pas vers {expected_schema}")
                all_correct = False
            else:
                logger.debug(f"[VIEW_SWITCH] Vue {view_name} OK -> {expected_schema}")

        if all_correct:
            logger.info(f"[VIEW_SWITCH] Toutes les vues pointent vers {expected_schema}")
        return all_correct

    def create_view_for_table(
        self,
        table_name: str,
        source_schema: str,
        commit: bool = True
    ) -> bool:
        """
        Crée ou remplace une vue pour une table spécifique.

        Utile lors de la création d'une nouvelle table pour ajouter
        immédiatement la vue correspondante.

        Args:
            table_name: Nom de la table
            source_schema: Schéma source (ex: 'splus_blue')
            commit: Si True, commit immédiatement

        Returns:
            True si succès
        """
        with PostgresConnectionManager(self.postgres_hook) as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()

            try:
                drop_sql = sql.SQL(
                    "DROP VIEW IF EXISTS {view_schema}.{table}"
                ).format(
                    view_schema=sql.Identifier(self.VIEW_SCHEMA),
                    table=sql.Identifier(table_name.lower())
                )
                cursor.execute(drop_sql)

                create_sql = sql.SQL(
                    "CREATE VIEW {view_schema}.{table} AS SELECT * FROM {source_schema}.{table}"
                ).format(
                    view_schema=sql.Identifier(self.VIEW_SCHEMA),
                    source_schema=sql.Identifier(source_schema),
                    table=sql.Identifier(table_name.lower())
                )
                cursor.execute(create_sql)

                if commit:
                    conn_mgr.commit()

                logger.info(f"[VIEW_SWITCH] Vue créée: {self.VIEW_SCHEMA}.{table_name} -> {source_schema}")
                return True

            except Exception as e:
                if commit:
                    conn_mgr.rollback()
                logger.error(f"[VIEW_SWITCH] Erreur création vue {table_name}: {e}")
                return False
            finally:
                cursor.close()

    def drop_view(self, table_name: str, commit: bool = True) -> bool:
        """
        Supprime une vue.

        Args:
            table_name: Nom de la vue à supprimer
            commit: Si True, commit immédiatement

        Returns:
            True si succès
        """
        with PostgresConnectionManager(self.postgres_hook) as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()

            try:
                drop_sql = sql.SQL("""
                    DROP VIEW IF EXISTS {view_schema}.{table}
                """).format(
                    view_schema=sql.Identifier(self.VIEW_SCHEMA),
                    table=sql.Identifier(table_name.lower())
                )
                cursor.execute(drop_sql)

                if commit:
                    conn_mgr.commit()

                logger.info(f"[VIEW_SWITCH] Vue supprimée: {self.VIEW_SCHEMA}.{table_name}")
                return True

            except Exception as e:
                if commit:
                    conn_mgr.rollback()
                logger.error(f"[VIEW_SWITCH] Erreur suppression vue {table_name}: {e}")
                return False
            finally:
                cursor.close()

    def get_current_target_schema(self) -> Optional[str]:
        """
        Détermine vers quel schéma les vues pointent actuellement.

        Analyse la première vue trouvée pour déterminer le schéma cible.

        Returns:
            Nom du schéma ('splus_blue' ou 'splus_green') ou None si pas de vues
        """
        query = """
            SELECT view_definition
            FROM information_schema.views
            WHERE table_schema = %s
            LIMIT 1
        """
        result = self.postgres_hook.get_first(query, parameters=(self.VIEW_SCHEMA,))

        if not result or not result[0]:
            return None

        view_def = result[0].lower()
        if 'splus_blue' in view_def:
            return 'splus_blue'
        elif 'splus_green' in view_def:
            return 'splus_green'
        else:
            logger.warning(f"[VIEW_SWITCH] Schéma cible non reconnu dans: {view_def[:100]}")
            return None
