"""
Tests unitaires pour AMUETableManager
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestTableManagerInit:
    """Tests pour l'initialisation de AMUETableManager"""

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_init_default_hook(self, mock_varmgr, mock_create_hook):
        """Utilise le hook par défaut si non fourni"""
        mock_varmgr.get.return_value = 'dev'
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        assert manager.postgres_hook == mock_postgres_hook
        mock_create_hook.assert_called_once()

    @patch('amue.operators.table_manager.VarMgr')
    def test_init_custom_hook(self, mock_varmgr):
        """Utilise le hook personnalisé si fourni"""
        mock_varmgr.get.return_value = 'dev'
        mock_postgres_hook = MagicMock()

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager(postgres_hook=mock_postgres_hook)

        assert manager.postgres_hook == mock_postgres_hook


class TestTableManagerValidation:
    """Tests pour la validation de structure"""

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_validate_structure_info_valid(self, mock_varmgr, mock_create_hook):
        """Structure valide ne lève pas d'erreur"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
            'primary_keys': 'id',
            'exists': False
        }

        # Ne doit pas lever d'exception
        manager._validate_structure_info(structure_info)

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_validate_structure_info_missing_fields(self, mock_varmgr, mock_create_hook):
        """Champs manquants lèvent une erreur"""
        mock_varmgr.get.return_value = 'dev'
        from airflow.exceptions import AirflowException

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS'
            # Manque columns, primary_keys, exists
        }

        with pytest.raises(AirflowException, match="Champs manquants"):
            manager._validate_structure_info(structure_info)

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_validate_structure_info_empty_columns(self, mock_varmgr, mock_create_hook):
        """Colonnes vides lèvent une erreur"""
        mock_varmgr.get.return_value = 'dev'
        from airflow.exceptions import AirflowException

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [],  # Vide
            'primary_keys': 'id',
            'exists': False
        }

        with pytest.raises(AirflowException, match="aucune colonne"):
            manager._validate_structure_info(structure_info)


class TestTableManagerProduction:
    """Tests pour l'environnement de production"""

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_production_table_exists(self, mock_varmgr, mock_create_hook):
        """En production, table existante OK"""
        mock_varmgr.get.return_value = 'production'

        from amue.operators.table_manager import AMUETableManager

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

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_production_table_missing(self, mock_varmgr, mock_create_hook):
        """En production, table manquante lève une erreur"""
        mock_varmgr.get.return_value = 'production'
        from airflow.exceptions import AirflowException

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        structure_info = {
            'table_name': 'CSKS',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
            'primary_keys': 'id',
            'exists': False
        }

        with pytest.raises(AirflowException, match="PRODUCTION.*Création interdite"):
            manager.manage_table(structure_info)


class TestTableManagerDevelopment:
    """Tests pour l'environnement de développement"""

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_dev_table_exists(self, mock_varmgr, mock_create_hook):
        """En dev, table existante OK"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

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

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_dev_table_created(self, mock_varmgr, mock_create_hook):
        """En dev, table manquante est créée"""
        mock_varmgr.get.return_value = 'dev'
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_manager import AMUETableManager

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


class TestTableManagerSQLGeneration:
    """Tests pour la génération SQL"""

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_create_table_sql(self, mock_varmgr, mock_create_hook):
        """Génération du SQL CREATE TABLE"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

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

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_create_table_sql_composite_pk(self, mock_varmgr, mock_create_hook):
        """Génération SQL avec clé primaire composite"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id1', 'type_postgres': 'INTEGER'},
            {'name': 'id2', 'type_postgres': 'INTEGER'},
            {'name': 'value', 'type_postgres': 'VARCHAR(50)'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, 'id1, id2')

        assert 'PRIMARY KEY (id1, id2)' in sql

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_create_table_sql_no_pk(self, mock_varmgr, mock_create_hook):
        """Génération SQL sans clé primaire"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'value', 'type_postgres': 'VARCHAR(50)'}
        ]

        sql = manager._build_create_table_sql('test_table', columns, '')

        assert 'PRIMARY KEY' not in sql

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_primary_key_constraint_single(self, mock_varmgr, mock_create_hook):
        """Contrainte PK simple"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('id')

        assert 'PRIMARY KEY (id)' in result

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_primary_key_constraint_composite(self, mock_varmgr, mock_create_hook):
        """Contrainte PK composite"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('ID, NAME, DATE')

        assert 'PRIMARY KEY (id, name, date)' in result

    @patch('amue.operators.table_manager.create_postgres_hook')
    @patch('amue.operators.table_manager.VarMgr')
    def test_build_primary_key_constraint_empty(self, mock_varmgr, mock_create_hook):
        """Contrainte PK vide"""
        mock_varmgr.get.return_value = 'dev'

        from amue.operators.table_manager import AMUETableManager

        manager = AMUETableManager()

        result = manager._build_primary_key_constraint('')

        assert result == ''


class TestTableManagementResult:
    """Tests pour TableManagementResult dataclass"""

    def test_result_dataclass(self):
        """Création d'un résultat"""
        from amue.operators.table_manager import TableManagementResult

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
        from amue.operators.table_manager import TableManagementResult

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
