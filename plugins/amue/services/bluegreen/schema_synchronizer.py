"""
Service de synchronisation des schémas Blue/Green.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module synchronise les données entre les schémas blue et green.
La synchronisation copie le contenu du schéma actif vers le schéma cible
avant un nouvel import, permettant ainsi d'avoir un point de rollback.

STRATÉGIE (Sync avant prochain import avec rollback) :
    1. Sync ACTIF -> CIBLE (mise à niveau avant import)
    2. Import nouvelles données -> CIBLE
    3. Switch vues -> CIBLE devient actif
    4. ACTIF (ancien) reste intact = SNAPSHOT pour rollback

================================================================================
SYNCHRONISATION
================================================================================

Pour chaque table :
    1. TRUNCATE table cible
    2. INSERT INTO cible SELECT * FROM source

La sync est effectuée table par table avec commit intermédiaire pour
permettre un monitoring de la progression.

================================================================================
"""
import logging
from typing import Dict, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from amue.utils.database.hooks import create_postgres_hook
from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

logger = logging.getLogger(__name__)


class SchemaSynchronizer:
    """
    Synchronise les données entre schémas blue et green.

    Responsabilités :
        - Copier les données d'un schéma vers l'autre
        - Gérer la progression table par table
        - Garantir la cohérence des données

    Example:
        >>> sync = SchemaSynchronizer()
        >>> result = sync.sync_schemas('splus_blue', 'splus_green')
        >>> print(f"Sync: {result['tables_synced']} tables, {result['rows_copied']} lignes")
    """

    def __init__(self, postgres_hook: PostgresHook = None):
        """
        Initialise le synchroniseur.

        Args:
            postgres_hook: Hook PostgreSQL (créé si non fourni)
        """
        self.postgres_hook = postgres_hook or create_postgres_hook(schema='public')
        self.bluegreen_manager = BlueGreenManager()

    def get_tables_to_sync(self, schema_name: str) -> List[str]:
        """
        Liste les tables à synchroniser dans un schéma.

        Args:
            schema_name: Nom du schéma source

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
        return [row[0] for row in result] if result else []

    def sync_table(
        self,
        table_name: str,
        source_schema: str,
        target_schema: str
    ) -> Dict:
        """
        Synchronise une table du schéma source vers le schéma cible.

        Opérations :
            1. TRUNCATE table cible
            2. INSERT INTO cible SELECT * FROM source

        Args:
            table_name: Nom de la table
            source_schema: Schéma source (ex: 'splus_blue')
            target_schema: Schéma cible (ex: 'splus_green')

        Returns:
            Résultat de la synchronisation :
            {
                'table_name': 'csks',
                'status': 'success' | 'error',
                'rows_copied': 1500,
                'error': None | 'message'
            }
        """
        logger.info(f"[SYNC] Table {table_name}: {source_schema} -> {target_schema}")

        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Vérifie que la table existe dans les deux schémas
            if not self._table_exists(table_name, source_schema):
                return {
                    'table_name': table_name,
                    'status': 'skipped',
                    'rows_copied': 0,
                    'error': f"Table absente du schéma source {source_schema}"
                }

            if not self._table_exists(table_name, target_schema):
                return {
                    'table_name': table_name,
                    'status': 'skipped',
                    'rows_copied': 0,
                    'error': f"Table absente du schéma cible {target_schema}"
                }

            # TRUNCATE cible
            truncate_sql = sql.SQL("TRUNCATE TABLE {schema}.{table}").format(
                schema=sql.Identifier(target_schema),
                table=sql.Identifier(table_name)
            )
            cursor.execute(truncate_sql)

            # INSERT SELECT
            insert_sql = sql.SQL("""
                INSERT INTO {target_schema}.{table}
                SELECT * FROM {source_schema}.{table}
            """).format(
                target_schema=sql.Identifier(target_schema),
                source_schema=sql.Identifier(source_schema),
                table=sql.Identifier(table_name)
            )
            cursor.execute(insert_sql)
            rows_copied = cursor.rowcount

            conn.commit()
            logger.info(f"[SYNC] Table {table_name}: {rows_copied} lignes copiées")

            return {
                'table_name': table_name,
                'status': 'success',
                'rows_copied': rows_copied,
                'error': None
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"[SYNC] Erreur sync {table_name}: {e}")
            return {
                'table_name': table_name,
                'status': 'error',
                'rows_copied': 0,
                'error': str(e)
            }
        finally:
            cursor.close()

    def sync_schemas(
        self,
        source_schema: str,
        target_schema: str,
        tables: Optional[List[str]] = None
    ) -> Dict:
        """
        Synchronise toutes les tables d'un schéma vers un autre.

        Args:
            source_schema: Schéma source (ex: 'splus_blue')
            target_schema: Schéma cible (ex: 'splus_green')
            tables: Liste de tables à synchroniser (toutes si None)

        Returns:
            Résultat global :
            {
                'status': 'success' | 'partial' | 'error',
                'source_schema': 'splus_blue',
                'target_schema': 'splus_green',
                'tables_synced': 30,
                'tables_failed': 0,
                'tables_skipped': 2,
                'total_rows_copied': 150000,
                'details': [...]
            }
        """
        logger.info(f"[SYNC] Démarrage sync: {source_schema} -> {target_schema}")

        # Liste des tables à synchroniser
        if tables is None:
            tables = self.get_tables_to_sync(source_schema)

        if not tables:
            logger.warning(f"[SYNC] Aucune table à synchroniser dans {source_schema}")
            return {
                'status': 'success',
                'source_schema': source_schema,
                'target_schema': target_schema,
                'tables_synced': 0,
                'tables_failed': 0,
                'tables_skipped': 0,
                'total_rows_copied': 0,
                'details': []
            }

        logger.info(f"[SYNC] {len(tables)} tables à synchroniser")

        # Synchronise chaque table
        results = []
        tables_synced = 0
        tables_failed = 0
        tables_skipped = 0
        total_rows = 0

        for table_name in tables:
            result = self.sync_table(table_name, source_schema, target_schema)
            results.append(result)

            if result['status'] == 'success':
                tables_synced += 1
                total_rows += result['rows_copied']
            elif result['status'] == 'error':
                tables_failed += 1
            else:
                tables_skipped += 1

        # Détermine le statut global
        if tables_failed == 0:
            status = 'success'
        elif tables_synced > 0:
            status = 'partial'
        else:
            status = 'error'

        logger.info(f"[SYNC] Terminé: {tables_synced} OK, {tables_failed} erreurs, {tables_skipped} skipped")
        logger.info(f"[SYNC] Total: {total_rows} lignes copiées")

        return {
            'status': status,
            'source_schema': source_schema,
            'target_schema': target_schema,
            'tables_synced': tables_synced,
            'tables_failed': tables_failed,
            'tables_skipped': tables_skipped,
            'total_rows_copied': total_rows,
            'details': results
        }

    def sync_active_to_target(self) -> Dict:
        """
        Synchronise le schéma actif vers le schéma cible.

        Raccourci utilisant BlueGreenManager pour déterminer les schémas.

        Returns:
            Résultat de la synchronisation
        """
        if not self.bluegreen_manager.is_enabled():
            logger.warning("[SYNC] Blue/green désactivé, sync ignorée")
            return {
                'status': 'skipped',
                'error': 'Blue/green mode not enabled'
            }

        source = self.bluegreen_manager.get_active_schema()
        target = self.bluegreen_manager.get_target_schema()

        logger.info(f"[SYNC] Sync automatique: {source} -> {target}")
        result = self.sync_schemas(source, target)

        if result['status'] in ('success', 'partial'):
            self.bluegreen_manager.mark_sync_completed()

        return result

    def _table_exists(self, table_name: str, schema_name: str) -> bool:
        """Vérifie si une table existe dans un schéma"""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            )
        """
        result = self.postgres_hook.get_first(query, parameters=(schema_name, table_name.lower()))
        return result[0] if result else False

    def compare_row_counts(
        self,
        schema1: str,
        schema2: str,
        tables: Optional[List[str]] = None
    ) -> Dict:
        """
        Compare le nombre de lignes entre deux schémas.

        Utile pour vérifier la synchronisation.

        Args:
            schema1: Premier schéma
            schema2: Deuxième schéma
            tables: Liste de tables à comparer (toutes si None)

        Returns:
            Comparaison :
            {
                'identical': True | False,
                'differences': [
                    {'table': 'csks', 'schema1_count': 100, 'schema2_count': 95},
                    ...
                ]
            }
        """
        if tables is None:
            tables = self.get_tables_to_sync(schema1)

        differences = []

        for table_name in tables:
            count1 = self._get_row_count(table_name, schema1)
            count2 = self._get_row_count(table_name, schema2)

            if count1 != count2:
                differences.append({
                    'table': table_name,
                    f'{schema1}_count': count1,
                    f'{schema2}_count': count2
                })

        return {
            'identical': len(differences) == 0,
            'differences': differences
        }

    def _get_row_count(self, table_name: str, schema_name: str) -> int:
        """Compte les lignes d'une table"""
        # Utilise une requête simple sans psycopg2.sql pour éviter les problèmes de contexte
        query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
        result = self.postgres_hook.get_first(query)
        return result[0] if result else 0
