"""
Tests unitaires pour SchemaSynchronizer
"""
import pytest
from unittest.mock import MagicMock, patch


class TestSchemaSynchronizerInit:
    """Tests pour l'initialisation de SchemaSynchronizer"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_init_default_hook(self, mock_create_hook, mock_bg_manager):
        """Utilise le hook par défaut si non fourni"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()

        assert sync.postgres_hook == mock_postgres_hook
        mock_create_hook.assert_called_once_with(schema='public')

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    def test_init_custom_hook(self, mock_bg_manager):
        """Utilise le hook personnalisé si fourni"""
        mock_postgres_hook = MagicMock()

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer(postgres_hook=mock_postgres_hook)

        assert sync.postgres_hook == mock_postgres_hook


class TestSchemaSynchronizerGetTables:
    """Tests pour la récupération des tables"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_get_tables_to_sync(self, mock_create_hook, mock_bg_manager):
        """Liste les tables à synchroniser"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks',),
            ('prps',),
            ('fmbl',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        tables = sync.get_tables_to_sync('splus_blue')

        assert tables == ['csks', 'prps', 'fmbl']

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_get_tables_to_sync_empty(self, mock_create_hook, mock_bg_manager):
        """Retourne liste vide si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        tables = sync.get_tables_to_sync('splus_blue')

        assert tables == []


class TestSchemaSynchronizerSyncTable:
    """Tests pour la synchronisation d'une table"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_table_success(self, mock_create_hook, mock_bg_manager):
        """Synchronise une table avec succès (TRUNCATE + INSERT)"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_first.return_value = (True,)  # Tables existent
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_table('csks', 'splus_blue', 'splus_green')

        assert result['status'] == 'success'
        assert result['rows_copied'] == 100
        assert result['table_name'] == 'csks'
        assert result['created'] is False
        mock_conn.commit.assert_called_once()
        # TRUNCATE + INSERT = 2 executes
        assert mock_cursor.execute.call_count == 2

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_table_source_missing(self, mock_create_hook, mock_bg_manager):
        """Skip si table source manquante"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)  # Source manquante
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_table('csks', 'splus_blue', 'splus_green')

        assert result['status'] == 'skipped'
        assert 'source' in result['error'].lower()

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_table_target_missing_creates_table(self, mock_create_hook, mock_bg_manager):
        """Crée la table cible si elle n'existe pas, puis INSERT"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 75
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        # Source existe, target n'existe pas
        mock_postgres_hook.get_first.side_effect = [(True,), (False,)]
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_table('csks', 'splus_blue', 'splus_green')

        assert result['status'] == 'success'
        assert result['created'] is True
        assert result['rows_copied'] == 75
        mock_conn.commit.assert_called_once()
        # CREATE TABLE + INSERT = 2 executes (pas de TRUNCATE)
        assert mock_cursor.execute.call_count == 2

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_table_error_rollback(self, mock_create_hook, mock_bg_manager):
        """Rollback en cas d'erreur"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL Error")
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_table('csks', 'splus_blue', 'splus_green')

        assert result['status'] == 'error'
        assert 'SQL Error' in result['error']
        mock_conn.rollback.assert_called_once()


class TestSchemaSynchronizerSyncSchemas:
    """Tests pour la synchronisation complète"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_schemas_all_success(self, mock_create_hook, mock_bg_manager):
        """Synchronise tous les schémas avec succès"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 50
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',), ('prps',)]
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_schemas('splus_blue', 'splus_green')

        assert result['status'] == 'success'
        assert result['tables_synced'] == 2
        assert result['tables_created'] == 0
        assert result['tables_failed'] == 0
        assert result['total_rows_copied'] == 100

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_schemas_with_created_tables(self, mock_create_hook, mock_bg_manager):
        """tables_created incrémenté quand une table est créée"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 30
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',)]
        # Source existe, target n'existe pas
        mock_postgres_hook.get_first.side_effect = [(True,), (False,)]
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_schemas('splus_blue', 'splus_green')

        assert result['status'] == 'success'
        assert result['tables_synced'] == 1
        assert result['tables_created'] == 1

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_schemas_partial(self, mock_create_hook, mock_bg_manager):
        """Statut partial si certaines tables échouent"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Premier appel OK, deuxième échoue
        mock_cursor.rowcount = 50
        call_count = [0]

        def execute_side_effect(*args):
            call_count[0] += 1
            if call_count[0] > 2:  # Après TRUNCATE + INSERT de la première table
                raise Exception("SQL Error")

        mock_cursor.execute.side_effect = execute_side_effect
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',), ('prps',)]
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_schemas('splus_blue', 'splus_green')

        assert result['status'] == 'partial'
        assert result['tables_synced'] == 1
        assert result['tables_failed'] == 1

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_schemas_empty(self, mock_create_hook, mock_bg_manager):
        """Retourne success si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_schemas('splus_blue', 'splus_green')

        assert result['status'] == 'success'
        assert result['tables_synced'] == 0


class TestSchemaSynchronizerActiveToTarget:
    """Tests pour sync_active_to_target"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_sync_active_to_target_success(self, mock_create_hook, mock_bg_manager):
        """Sync automatique avec succès"""
        mock_manager = MagicMock()
        mock_manager.get_active_schema.return_value = 'splus_blue'
        mock_manager.get_target_schema.return_value = 'splus_green'
        mock_bg_manager.return_value = mock_manager

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 50
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',)]
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.sync_active_to_target()

        assert result['status'] == 'success'
        mock_manager.mark_sync_completed.assert_called_once()


class TestSchemaSynchronizerCompare:
    """Tests pour la comparaison des schémas"""

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_compare_row_counts_identical(self, mock_create_hook, mock_bg_manager):
        """Schémas identiques"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [('csks',)]
        # Les deux schémas ont le même nombre de lignes
        mock_postgres_hook.get_first.return_value = (100,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.compare_row_counts('splus_blue', 'splus_green')

        assert result['identical'] is True
        assert len(result['differences']) == 0

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_compare_row_counts_different(self, mock_create_hook, mock_bg_manager):
        """Schémas différents"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [('csks',)]
        # Premier appel 100, deuxième 95
        mock_postgres_hook.get_first.side_effect = [(100,), (95,)]
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        result = sync.compare_row_counts('splus_blue', 'splus_green')

        assert result['identical'] is False
        assert len(result['differences']) == 1

    @patch('common.services.bluegreen.schema_synchronizer.BlueGreenManager')
    @patch('common.services.bluegreen.schema_synchronizer.create_postgres_hook')
    def test_get_row_count_uses_identifier(self, mock_create_hook, mock_bg_manager):
        """_get_row_count passe un objet sql.Composed à get_first (pas une f-string)"""
        from psycopg2 import sql as pgsql

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (42,)
        mock_create_hook.return_value = mock_postgres_hook

        from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer

        sync = SchemaSynchronizer()
        count = sync._get_row_count('csks', 'splus_blue')

        assert count == 42
        call_args = mock_postgres_hook.get_first.call_args
        query_arg = call_args[0][0]
        # La requête doit être un objet sql.Composed, pas une chaîne
        assert isinstance(query_arg, pgsql.Composed), (
            f"Attendu sql.Composed, obtenu {type(query_arg).__name__!r}"
        )
