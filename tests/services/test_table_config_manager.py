"""
Tests unitaires pour TableConfigManager
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


def make_manager(mock_hook=None):
    from amue.services.table_config_manager import TableConfigManager
    if mock_hook is None:
        mock_hook = MagicMock()
    return TableConfigManager(postgres_hook=mock_hook), mock_hook


class TestTableConfigManagerGetTablesConfig:
    """Tests pour get_tables_config"""

    def test_get_tables_config_returns_list(self):
        """Retourne la liste des tables depuis la BDD"""
        manager, hook = make_manager()
        hook.get_records.return_value = [
            ('CSKS', True, 'BUKRS,KOSTL', '', 'fp_api', 'fp_ul'),
            ('COBK', True, '', '', '', ''),
        ]

        result = manager.get_tables_config()

        assert len(result) == 2
        assert result[0]['name'] == 'CSKS'
        assert result[0]['enable'] is True
        assert result[0]['primary_key'] == 'BUKRS,KOSTL'
        assert result[0]['fingerprint_API'] == 'fp_api'
        assert result[0]['fingerprint_UL'] == 'fp_ul'
        assert 'last_import' not in result[0]

    def test_get_tables_config_empty(self):
        """Retourne liste vide si aucune table"""
        manager, hook = make_manager()
        hook.get_records.return_value = []

        result = manager.get_tables_config()

        assert result == []

    def test_get_tables_config_raises_on_db_error(self):
        """Propagation de l'exception si erreur BDD"""
        manager, hook = make_manager()
        hook.get_records.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            manager.get_tables_config()


class TestTableConfigManagerGetTableMetadata:
    """Tests pour get_table_metadata"""

    def test_get_table_metadata_found(self):
        """Retourne le dict d'une table existante"""
        manager, hook = make_manager()
        hook.get_first.return_value = ('CSKS', True, 'BUKRS,KOSTL', 'AEDAT', 'fp_api', 'fp_ul')

        result = manager.get_table_metadata('CSKS')

        assert result is not None
        assert result['name'] == 'CSKS'
        assert result['primary_key'] == 'BUKRS,KOSTL'
        assert result['delta'] == 'AEDAT'
        assert 'last_import' not in result

    def test_get_table_metadata_uppercase(self):
        """Cherche en majuscules"""
        manager, hook = make_manager()
        hook.get_first.return_value = None

        result = manager.get_table_metadata('csks')

        hook.get_first.assert_called_once()
        call_args = hook.get_first.call_args
        assert call_args[1]['parameters'] == ('CSKS',)

    def test_get_table_metadata_not_found(self):
        """Retourne None si table non trouvée"""
        manager, hook = make_manager()
        hook.get_first.return_value = None

        result = manager.get_table_metadata('UNKNOWN')

        assert result is None

    def test_get_table_metadata_returns_none_on_error(self):
        """Retourne None en cas d'erreur BDD"""
        manager, hook = make_manager()
        hook.get_first.side_effect = Exception("DB error")

        result = manager.get_table_metadata('CSKS')

        assert result is None


class TestTableConfigManagerSaveTablesConfig:
    """Tests pour save_tables_config"""

    def test_save_tables_config_batch(self):
        """UPDATE batch de plusieurs tables"""
        manager, hook = make_manager()
        tables = [
            {'name': 'CSKS', 'fingerprint_API': 'fp1', 'fingerprint_UL': 'fp2', 'primary_key': 'BUKRS,KOSTL'},
            {'name': 'COBK', 'fingerprint_API': 'fp3', 'fingerprint_UL': 'fp4', 'primary_key': ''},
        ]

        manager.save_tables_config(tables)

        # Un run par table
        assert hook.run.call_count == 2

    def test_save_tables_config_empty_list(self):
        """Aucun appel si liste vide"""
        manager, hook = make_manager()

        manager.save_tables_config([])

        hook.run.assert_not_called()

    def test_save_tables_config_raises_on_error(self):
        """Propagation de l'exception si erreur BDD"""
        manager, hook = make_manager()
        hook.run.side_effect = Exception("DB error")

        tables = [
            {'name': 'CSKS', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''},
        ]

        with pytest.raises(Exception, match="DB error"):
            manager.save_tables_config(tables)

    def test_save_tables_config_skips_empty_name(self):
        """Tables sans nom ignorées"""
        manager, hook = make_manager()
        tables = [
            {'name': '', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''},
        ]

        manager.save_tables_config(tables)

        hook.run.assert_not_called()


class TestTableConfigManagerSavePrimaryKeys:
    """Tests pour save_primary_keys"""

    def test_save_primary_keys(self):
        """Sauvegarde les PKs d'une table"""
        manager, hook = make_manager()

        manager.save_primary_keys('CSKS', 'BUKRS,KOSTL')

        hook.run.assert_called_once()
        call_args = hook.run.call_args
        assert call_args[1]['parameters'] == ('BUKRS,KOSTL', 'CSKS')

    def test_save_primary_keys_uppercase(self):
        """table_name converti en majuscules"""
        manager, hook = make_manager()

        manager.save_primary_keys('csks', 'BUKRS,KOSTL')

        call_args = hook.run.call_args
        assert call_args[1]['parameters'][1] == 'CSKS'


class TestTableConfigManagerResetTableMetadata:
    """Tests pour reset_table_metadata"""

    def test_reset_table_metadata_success(self):
        """Réinitialise fingerprints uniquement (plus de last_import)"""
        manager, hook = make_manager()

        result = manager.reset_table_metadata('CSKS')

        assert result is True
        hook.run.assert_called_once()
        call_args = hook.run.call_args
        assert call_args[1]['parameters'] == ('CSKS',)

    def test_reset_table_metadata_returns_false_on_error(self):
        """Retourne False si erreur BDD"""
        manager, hook = make_manager()
        hook.run.side_effect = Exception("DB error")

        result = manager.reset_table_metadata('CSKS')

        assert result is False


class TestTableConfigManagerRowToDict:
    """Tests pour _row_to_dict"""

    def test_row_to_dict_basic(self):
        """Conversion de base sans champs null"""
        from amue.services.table_config_manager import TableConfigManager

        row = ('CSKS', True, 'BUKRS', 'AEDAT', 'fp_api', 'fp_ul')
        result = TableConfigManager._row_to_dict(row)

        assert result['name'] == 'CSKS'
        assert result['enable'] is True
        assert result['primary_key'] == 'BUKRS'
        assert result['delta'] == 'AEDAT'
        assert result['fingerprint_API'] == 'fp_api'
        assert result['fingerprint_UL'] == 'fp_ul'
        assert 'last_import' not in result

    def test_row_to_dict_none_fields_become_empty(self):
        """primary_key, delta, fingerprints à None → chaîne vide"""
        from amue.services.table_config_manager import TableConfigManager

        row = ('CSKS', True, None, None, None, None)
        result = TableConfigManager._row_to_dict(row)

        assert result['primary_key'] == ''
        assert result['delta'] == ''
        assert result['fingerprint_API'] == ''
        assert result['fingerprint_UL'] == ''
