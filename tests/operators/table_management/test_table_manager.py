"""
Tests unitaires pour AMUETableManager
"""
import pytest
from unittest.mock import MagicMock, patch


class TestTableManagerInit:
    """Tests pour l'initialisation de AMUETableManager"""

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_init_default_hook(self, mock_create_hook):
        """Utilise le hook par défaut si non fourni"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        assert manager.postgres_hook == mock_postgres_hook
        mock_create_hook.assert_called_once()

    def test_init_custom_hook(self):
        """Utilise le hook personnalisé si fourni"""
        mock_postgres_hook = MagicMock()

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager(postgres_hook=mock_postgres_hook)

        assert manager.postgres_hook == mock_postgres_hook


class TestTableManagerValidation:
    """Tests pour la validation de structure"""

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_validate_structure_info_valid(self, mock_create_hook):
        """Structure valide ne lève pas d'erreur"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
            'primary_keys': 'id',
            'exists': False
        }

        # Ne doit pas lever d'exception
        manager._validate_structure_info(structure_info)

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_validate_structure_info_missing_fields(self, mock_create_hook):
        """Champs manquants lèvent une erreur"""
        from amue.exceptions import AMUESchemaError

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS'
            # Manque columns, primary_keys, exists
        }

        with pytest.raises(AMUESchemaError, match="Champs manquants"):
            manager._validate_structure_info(structure_info)

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_validate_structure_info_empty_columns(self, mock_create_hook):
        """Colonnes vides lèvent une erreur"""
        from amue.exceptions import AMUESchemaError

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [],  # Vide
            'primary_keys': 'id',
            'exists': False
        }

        with pytest.raises(AMUESchemaError, match="aucune colonne"):
            manager._validate_structure_info(structure_info)


class TestTableManagerManageTable:
    """Tests pour manage_table (basé uniquement sur l'existence en base)"""

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_table_exists_uses_existing(self, mock_create_hook):
        """Table existante est réutilisée"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
            'primary_keys': 'id',
            'exists': True
        }

        result = manager.manage_table(structure_info)

        assert result['status'] == 'success'
        assert result['created'] is False

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_table_missing_is_created(self, mock_create_hook):
        """Table absente est créée automatiquement"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [
                {'name': 'id', 'type_postgres': 'INTEGER'},
                {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
            ],
            'primary_keys': 'id',
            'exists': False
        }

        result = manager.manage_table(structure_info)

        assert result['status'] == 'success'
        assert result['created'] is True
        mock_postgres_hook.run.assert_called_once()

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_table_exists_calls_ensure_meta(self, mock_create_hook):
        """Table existante appelle ensure_meta_columns"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
            'primary_keys': 'id',
            'exists': True
        }

        result = manager.manage_table(structure_info)

        assert result['status'] == 'success'
        # Vérifie que ensure_meta_columns a été appelé
        mock_postgres_hook.run.assert_called()


class TestTableManagerSQLGeneration:
    """Tests pour la génération SQL"""

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_create_table_sql(self, mock_create_hook):
        """Génération du SQL CREATE TABLE"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'name', 'type_postgres': 'VARCHAR(50)'},
            {'name': 'created_at', 'type_postgres': 'TIMESTAMP'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, 'id')

        assert 'DROP TABLE IF EXISTS test_table CASCADE' in sql
        assert 'CREATE TABLE test_table' in sql
        assert 'id INTEGER' in sql
        assert 'name VARCHAR(50)' in sql
        assert 'created_at TIMESTAMP' in sql
        assert 'PRIMARY KEY (id)' in sql

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_create_table_sql_composite_pk(self, mock_create_hook):
        """Génération SQL avec clé primaire composite"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id1', 'type_postgres': 'INTEGER'},
            {'name': 'id2', 'type_postgres': 'INTEGER'},
            {'name': 'value', 'type_postgres': 'VARCHAR(50)'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, 'id1, id2')

        assert 'PRIMARY KEY (id1, id2)' in sql

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_create_table_sql_no_pk(self, mock_create_hook):
        """Génération SQL sans clé primaire"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'value', 'type_postgres': 'VARCHAR(50)'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, '')

        assert 'PRIMARY KEY' not in sql

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_primary_key_constraint_single(self, mock_create_hook):
        """Contrainte PK simple"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('id')

        assert 'PRIMARY KEY (id)' in result

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_primary_key_constraint_composite(self, mock_create_hook):
        """Contrainte PK composite"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('ID, NAME, DATE')

        assert 'PRIMARY KEY (id, name, date)' in result

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_primary_key_constraint_empty(self, mock_create_hook):
        """Contrainte PK vide"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('')

        assert result == ''


class TestTableManagerMetaColumns:
    """Tests pour les meta colonnes _source et _imported_at"""

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_ensure_meta_columns(self, mock_create_hook):
        """ensure_meta_columns ajoute les colonnes"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()
        manager.ensure_meta_columns('test_table')

        # Vérifie que run a été appelé avec les ALTER TABLE
        mock_postgres_hook.run.assert_called_once()
        sql_called = mock_postgres_hook.run.call_args[0][0]
        assert '_source' in sql_called
        assert '_imported_at' in sql_called
        assert 'ADD COLUMN IF NOT EXISTS' in sql_called

    @patch('amue.operators.table_management.table_manager.create_postgres_hook')
    def test_build_create_table_sql_includes_meta_columns(self, mock_create_hook):
        """DDL inclut les meta colonnes"""
        from amue.operators.table_management.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, 'id')

        assert '_source VARCHAR(50)' in sql
        assert '_imported_at TIMESTAMP' in sql
        assert "DEFAULT 'sifac_plus'" in sql


class TestTableManagementResult:
    """Tests pour TableManagementResult dataclass"""

    def test_result_dataclass(self):
        """Création d'un résultat"""
        from amue.operators.table_management.table_manager import TableManagementResult

        result = TableManagementResult(
            table_name='csks',
            columns=['id', 'name'],
            primary_keys='id',
            created=True,
            status='success'
        )

        assert result.table_name == 'csks'
        assert result.columns == ['id', 'name']
        assert result.primary_keys == 'id'
        assert result.created is True
        assert result.status == 'success'
        assert result.error is None

    def test_result_dataclass_with_error(self):
        """Création d'un résultat avec erreur"""
        from amue.operators.table_management.table_manager import TableManagementResult

        result = TableManagementResult(
            table_name='csks',
            columns=[],
            primary_keys='',
            created=False,
            status='error',
            error='Table creation failed'
        )

        assert result.status == 'error'
        assert result.error == 'Table creation failed'


class TestTableManagerMetaColumns:
    """Tests pour ensure_meta_columns"""

    def test_ensure_meta_columns_sql_content(self):
        """hook.run reçoit une string SQL valide avec ALTER TABLE et les deux meta colonnes"""
        from amue.operators.table_management.table_manager import AMUETableManager

        mock_hook = MagicMock()
        manager = AMUETableManager(postgres_hook=mock_hook)

        manager.ensure_meta_columns('csks')

        mock_hook.run.assert_called_once()
        arg = mock_hook.run.call_args[0][0]
        assert isinstance(arg, str), f"hook.run doit recevoir une str, pas {type(arg).__name__}"
        assert 'ALTER TABLE' in arg
        assert '_source' in arg
        assert '_imported_at' in arg
