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
from pathlib import Path
from typing import Dict, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from common.utils.database.hooks import create_postgres_hook
from common.utils.database.connection_manager import PostgresConnectionManager
from common.utils.database.schema_utils import list_tables, list_views

logger = logging.getLogger(__name__)

CUSTOM_VIEWS_DIR = Path(__file__).parents[4] / "scripts" / "sql" / "custom_views"
VALID_TARGET_SCHEMAS = {"splus_blue", "splus_green"}


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

    def __init__(self, postgres_hook: PostgresHook = None, custom_views_dir: Path = CUSTOM_VIEWS_DIR):
        """
        Initialise le ViewSwitcher.

        Args:
            postgres_hook: Hook PostgreSQL (créé si non fourni)
            custom_views_dir: Répertoire contenant les fichiers .sql de vues custom
        """
        self.postgres_hook = postgres_hook or create_postgres_hook(schema='public')
        logger.debug(f"[VIEW_SWITCH] custom_views_dir: {custom_views_dir}")
        self.custom_views_dir = custom_views_dir

    def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """
        Liste toutes les tables dans un schéma.

        Args:
            schema_name: Nom du schéma (ex: 'splus_blue')

        Returns:
            Liste des noms de tables
        """
        tables = list_tables(self.postgres_hook, schema_name)
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
        views = list_views(self.postgres_hook, schema_name)
        logger.info(f"[VIEW_SWITCH] {len(views)} vues trouvées dans {schema_name}")
        return views

    def switch_views_to_schema(self, target_schema: str) -> bool:
        """
        Bascule toutes les vues vers le schéma cible.

        Cette opération est atomique : soit toutes les vues sont basculées,
        soit aucune en cas d'erreur.

        Args:
            target_schema: Schéma cible ('splus_blue' ou 'splus_green')

        Returns:
            True si le switch a réussi, False sinon

        Raises:
            ValueError: Si target_schema n'est pas un schéma valide
        """
        if target_schema not in VALID_TARGET_SCHEMAS:
            raise ValueError(
                f"Schéma invalide: {target_schema!r}. Attendu: {VALID_TARGET_SCHEMAS}"
            )

        logger.info(f"[VIEW_SWITCH] Switch des vues vers {target_schema}")

        # Récupère la liste des tables dans le schéma cible
        tables = self.get_tables_in_schema(target_schema)

        if not tables:
            logger.warning(f"[VIEW_SWITCH] Aucune table dans {target_schema}, abandon")
            return False

        # Récupère toutes les colonnes en une seule requête (évite le N+1)
        columns_dict = self._get_all_view_columns(tables, target_schema)

        with PostgresConnectionManager(self.postgres_hook) as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()

            try:
                # DROP puis CREATE chaque vue dans une transaction atomique
                for table_name in tables:
                    drop_sql = sql.SQL(
                        "DROP VIEW IF EXISTS {view_schema}.{table}"
                    ).format(
                        view_schema=sql.Identifier(self.VIEW_SCHEMA),
                        table=sql.Identifier(table_name)
                    )
                    cursor.execute(drop_sql)

                    columns = columns_dict.get(table_name, [])
                    if not columns:
                        logger.warning(f"[VIEW_SWITCH] Aucune colonne pour {table_name}, fallback SELECT *")

                    create_sql = self._build_view_sql(table_name, target_schema, columns)
                    cursor.execute(create_sql)
                    logger.debug(f"[VIEW_SWITCH] Vue {self.VIEW_SCHEMA}.{table_name} -> {target_schema}.{table_name}")

                # Commit atomique des vues standard
                conn_mgr.commit()
                logger.info(f"[VIEW_SWITCH] SUCCESS - {len(tables)} vues basculées vers {target_schema}")

            except Exception as e:
                conn_mgr.rollback()
                logger.error(f"[VIEW_SWITCH] ERREUR - Rollback: {e}")
                cursor.close()
                return False

            # Vues custom : une par une, best-effort (échec non bloquant)
            if self.custom_views_dir.exists():
                ok = ko = 0
                for sql_file in sorted(self.custom_views_dir.glob("*.sql")):
                    content = sql_file.read_text(encoding="utf-8").replace("{target_schema}", target_schema)
                    try:
                        cursor.execute(content)
                        conn_mgr.commit()
                        logger.info(f"[VIEW_SWITCH] Vue custom OK: {sql_file.name}")
                        ok += 1
                    except Exception as e:
                        conn_mgr.rollback()
                        logger.warning(f"[VIEW_SWITCH] Vue custom ÉCHEC ({sql_file.name}): {e}")
                        ko += 1
                if ok or ko:
                    logger.info(f"[VIEW_SWITCH] Vues custom: {ok} OK, {ko} en échec")

            cursor.close()
            return True

    def refresh_custom_views(self, target_schema: str) -> Dict:
        """
        Recrée toutes les vues custom du répertoire custom_views_dir vers le schéma cible.

        Opération indépendante de switch_views_to_schema() : ouvre sa propre connexion
        et traite chaque vue en best-effort (un échec n'arrête pas les suivantes).

        Args:
            target_schema: Schéma cible ('splus_blue' ou 'splus_green')

        Returns:
            {"ok": int, "ko": int, "target_schema": str, "files_processed": List[str]}
            files_processed contient uniquement les fichiers exécutés avec succès.

        Raises:
            ValueError: Si target_schema n'est pas un schéma valide
            FileNotFoundError: Si le répertoire custom_views est absent
        """
        if target_schema not in VALID_TARGET_SCHEMAS:
            raise ValueError(
                f"Schéma invalide: {target_schema!r}. Attendu: {VALID_TARGET_SCHEMAS}"
            )
        if not self.custom_views_dir.exists():
            raise FileNotFoundError(
                f"Répertoire custom_views introuvable : {self.custom_views_dir}"
            )

        ok = ko = 0
        files_processed: List[str] = []
        files_failed: List[Dict] = []

        with PostgresConnectionManager(self.postgres_hook) as conn_mgr:
            conn = conn_mgr.get_connection()
            cursor = conn.cursor()
            try:
                for sql_file in sorted(self.custom_views_dir.glob("*.sql")):
                    content = sql_file.read_text(encoding="utf-8").replace(
                        "{target_schema}", target_schema
                    )
                    try:
                        cursor.execute(content)
                        conn_mgr.commit()
                        logger.info(f"[REFRESH_VIEWS] Vue custom OK : {sql_file.name}")
                        ok += 1
                        files_processed.append(sql_file.name)
                    except Exception as e:
                        conn_mgr.rollback()
                        logger.warning(
                            f"[REFRESH_VIEWS] Vue custom ÉCHEC ({sql_file.name}) : {e}"
                        )
                        ko += 1
                        files_failed.append({"filename": sql_file.name, "error": str(e)})
            finally:
                cursor.close()

        logger.info(f"[REFRESH_VIEWS] {ok} OK, {ko} en échec → {target_schema}")
        return {
            "ok": ok,
            "ko": ko,
            "target_schema": target_schema,
            "files_processed": files_processed,
            "files_failed": files_failed,
        }

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

                columns = self._get_view_columns(table_name.lower(), source_schema)
                if not columns:
                    logger.warning(f"[VIEW_SWITCH] Aucune colonne pour {table_name}, fallback SELECT *")

                create_sql = self._build_view_sql(table_name.lower(), source_schema, columns)
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

    def _get_view_columns(self, table_name: str, schema_name: str) -> List[str]:
        """
        Retourne les colonnes à exposer dans la vue, en excluant les colonnes techniques.

        Les colonnes '_source' et '_imported_at' sont des métadonnées internes
        qui ne doivent pas être visibles dans le schéma splus.

        Args:
            table_name: Nom de la table
            schema_name: Schéma où chercher les colonnes

        Returns:
            Liste des noms de colonnes (sans _source ni _imported_at)
        """
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name NOT IN ('_source', '_imported_at')
            ORDER BY ordinal_position
        """
        result = self.postgres_hook.get_records(query, parameters=(schema_name, table_name))
        return [row[0] for row in result] if result else []

    def _get_all_view_columns(self, table_names: List[str], schema_name: str) -> Dict[str, List[str]]:
        """
        Retourne les colonnes à exposer dans la vue pour chaque table, en une seule requête.

        Variante batch de _get_view_columns : une seule requête SQL pour N tables
        au lieu de N requêtes. Exclut les colonnes techniques '_source' et '_imported_at'.

        Args:
            table_names: Liste des noms de tables
            schema_name: Schéma où chercher les colonnes

        Returns:
            Dict {table_name: [colonnes]} (ordre d'ordinal_position préservé)
        """
        if not table_names:
            return {}
        query = """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = ANY(%s)
              AND column_name NOT IN ('_source', '_imported_at')
            ORDER BY table_name, ordinal_position
        """
        result = self.postgres_hook.get_records(query, parameters=(schema_name, list(table_names)))
        columns_dict: Dict[str, List[str]] = {}
        for tname, col_name in (result or []):
            columns_dict.setdefault(tname, []).append(col_name)
        return columns_dict

    def _build_view_sql(self, table_name: str, target_schema: str, columns: List[str]):
        """
        Construit le SQL CREATE VIEW pour une table.

        Args:
            table_name: Nom de la table (et de la vue résultante)
            target_schema: Schéma source des données
            columns: Colonnes à exposer (SELECT * si vide)

        Returns:
            Objet sql.Composed prêt à être exécuté
        """
        if not columns:
            col_sql = sql.SQL("*")
        else:
            col_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
        return sql.SQL(
            "CREATE VIEW {view_schema}.{table} AS SELECT {columns} FROM {target_schema}.{table}"
        ).format(
            view_schema=sql.Identifier(self.VIEW_SCHEMA),
            table=sql.Identifier(table_name),
            columns=col_sql,
            target_schema=sql.Identifier(target_schema)
        )

    def _load_custom_view_sqls(self, target_schema: str) -> List[str]:
        """
        Charge les fichiers .sql du répertoire custom_views et substitue le schéma cible.

        Les fichiers sont traités par ordre alphabétique (un préfixe numérique permet
        de contrôler les dépendances). Le placeholder `{target_schema}` dans chaque
        fichier est remplacé par le schéma cible (ex: 'splus_green').

        Args:
            target_schema: Schéma cible à injecter (ex: 'splus_blue' ou 'splus_green')

        Returns:
            Liste des SQL prêts à être exécutés, dans l'ordre alphabétique des fichiers
        """
        sqls = []
        if not self.custom_views_dir.exists():
            return sqls
        for sql_file in sorted(self.custom_views_dir.glob("*.sql")):
            content = sql_file.read_text(encoding="utf-8")
            sqls.append(content.replace("{target_schema}", target_schema))
            logger.info(f"[VIEW_SWITCH] Vue custom chargée: {sql_file.name}")
        return sqls

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
              AND (view_definition LIKE '%%splus_blue%%' OR view_definition LIKE '%%splus_green%%')
            ORDER BY table_name
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
