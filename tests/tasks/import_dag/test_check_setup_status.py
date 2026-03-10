"""Tests unitaires pour la task check_setup_status."""
import pytest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException


def _make_table(name, **kwargs):
    base = {'name': name, 'target_schema': 'splus_green'}
    base.update(kwargs)
    return base


def _make_metadata(status='ready', primary_key='bukrs,kostl'):
    return {'setup_status': status, 'primary_key': primary_key}


class TestCheckSetupStatus:
    """Tests pour la task de vérification du statut de setup."""

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_ready_table_passes_through(self, MockMgr):
        """Une table 'ready' est incluse dans la liste retournée."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        MockMgr.return_value.get_table_metadata.return_value = _make_metadata('ready', 'bukrs')
        tables = [_make_table('csks')]

        result = check_setup_status.function(tables)

        assert len(result) == 1
        assert result[0]['name'] == 'csks'

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_primary_key_enriched_from_db(self, MockMgr):
        """primary_key est enrichi depuis la config DB."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        MockMgr.return_value.get_table_metadata.return_value = _make_metadata('ready', 'bukrs,kostl')
        tables = [_make_table('csks', primary_key='')]

        result = check_setup_status.function(tables)

        assert result[0]['primary_key'] == 'bukrs,kostl'

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_pending_table_raises_airflow_exception(self, MockMgr):
        """Une table 'pending' lève AirflowException (fail-fast)."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        MockMgr.return_value.get_table_metadata.return_value = _make_metadata('pending')
        tables = [_make_table('csks')]

        with pytest.raises(AirflowException, match="pending"):
            check_setup_status.function(tables)

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_blocked_table_raises_airflow_exception(self, MockMgr):
        """Une table 'blocked' lève AirflowException (changement structure)."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        MockMgr.return_value.get_table_metadata.return_value = _make_metadata('blocked')
        tables = [_make_table('csks')]

        with pytest.raises(AirflowException, match="blocked"):
            check_setup_status.function(tables)

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_missing_table_raises_airflow_exception(self, MockMgr):
        """Une table absente de la config lève AirflowException."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        MockMgr.return_value.get_table_metadata.return_value = None
        tables = [_make_table('unknown_table')]

        with pytest.raises(AirflowException, match="introuvable"):
            check_setup_status.function(tables)

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_multiple_errors_aggregated(self, MockMgr):
        """Plusieurs erreurs sont agrégées dans une seule AirflowException."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        def side_effect(name):
            return _make_metadata('pending') if name == 'csks' else _make_metadata('blocked')

        MockMgr.return_value.get_table_metadata.side_effect = side_effect
        tables = [_make_table('csks'), _make_table('lfa1')]

        with pytest.raises(AirflowException) as exc_info:
            check_setup_status.function(tables)

        assert '2 table(s)' in str(exc_info.value)

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_mixed_ready_and_errors(self, MockMgr):
        """Les tables non-prêtes font échouer même si d'autres sont prêtes."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        def side_effect(name):
            return _make_metadata('ready') if name == 'csks' else _make_metadata('pending')

        MockMgr.return_value.get_table_metadata.side_effect = side_effect
        tables = [_make_table('csks'), _make_table('lfa1')]

        with pytest.raises(AirflowException):
            check_setup_status.function(tables)

    @patch('amue.tasks.import_dag.check_setup_status.TableConfigManager')
    def test_empty_table_list_returns_empty(self, MockMgr):
        """Une liste vide retourne une liste vide."""
        from amue.tasks.import_dag.check_setup_status import check_setup_status

        result = check_setup_status.function([])

        assert result == []
