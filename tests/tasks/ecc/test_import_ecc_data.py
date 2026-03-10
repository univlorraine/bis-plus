"""Tests unitaires pour la task import_ecc_data."""
import pytest
from unittest.mock import MagicMock, patch, call


def _make_table_config(**kwargs):
    base = {
        'table_name': 'lfa1',
        'ecc_query': 'SELECT * FROM LFA1',
        'primary_keys': ['lifnr'],
        'target_schema': 'splus_blue',
        'source': 'ecc',
        'protected_source': 'sifac_plus',
    }
    base.update(kwargs)
    return base


class TestImportEccData:
    """Tests pour la task d'import Oracle → PostgreSQL."""

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=100)
    @patch('ecc.tasks.import_dag.import_data.AMUEBatchInserter')
    @patch('ecc.tasks.import_dag.import_data.AMUETableManager')
    @patch('ecc.tasks.import_dag.import_data.create_postgres_hook')
    @patch('ecc.tasks.import_dag.import_data.ECCSourceHook')
    def test_returns_expected_keys(self, MockECC, mock_pg, MockMgr, MockInserter, mock_batch_size):
        """Le résultat contient les clés attendues."""
        from ecc.tasks.import_dag.import_data import import_ecc_data

        mock_pg.return_value.get_first.return_value = (True,)
        MockECC.return_value.execute_query.return_value = (['lifnr', 'name1'], iter([]))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'

        result = import_ecc_data.function(_make_table_config())

        assert 'table_name' in result
        assert 'rows_fetched' in result
        assert 'rows_inserted' in result
        assert 'rows_updated' in result
        assert 'rows_skipped' in result
        assert result['status'] == 'success'
        assert result['target_schema'] == 'splus_blue'

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=100)
    @patch('ecc.tasks.import_dag.import_data.AMUEBatchInserter')
    @patch('ecc.tasks.import_dag.import_data.AMUETableManager')
    @patch('ecc.tasks.import_dag.import_data.create_postgres_hook')
    @patch('ecc.tasks.import_dag.import_data.ECCSourceHook')
    def test_empty_oracle_result_returns_zero_rows(self, MockECC, mock_pg, MockMgr, MockInserter, mock_batch_size):
        """Aucune ligne Oracle → rows_fetched=0."""
        from ecc.tasks.import_dag.import_data import import_ecc_data

        mock_pg.return_value.get_first.return_value = (True,)
        MockECC.return_value.execute_query.return_value = (['lifnr'], iter([]))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'

        result = import_ecc_data.function(_make_table_config())

        assert result['rows_fetched'] == 0

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=2)
    @patch('ecc.tasks.import_dag.import_data.AMUEBatchInserter')
    @patch('ecc.tasks.import_dag.import_data.AMUETableManager')
    @patch('ecc.tasks.import_dag.import_data.create_postgres_hook')
    @patch('ecc.tasks.import_dag.import_data.ECCSourceHook')
    def test_rows_fetched_counts_all_rows(self, MockECC, mock_pg, MockMgr, MockInserter, mock_batch_size):
        """rows_fetched compte toutes les lignes Oracle."""
        from ecc.tasks.import_dag.import_data import import_ecc_data

        rows = [('LF001',), ('LF002',), ('LF003',)]
        mock_pg.return_value.get_first.return_value = (True,)
        MockECC.return_value.execute_query.return_value = (['lifnr'], iter(rows))
        mock_conn = MagicMock()
        MockInserter.return_value.get_connection.return_value = mock_conn
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'
        MockInserter.return_value.execute_batch.return_value = {
            'rows_inserted': 2, 'rows_updated': 0, 'rows_affected': 2, 'batch_size': 2
        }

        result = import_ecc_data.function(_make_table_config())

        assert result['rows_fetched'] == 3

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=100)
    @patch('ecc.tasks.import_dag.import_data.AMUEBatchInserter')
    @patch('ecc.tasks.import_dag.import_data.AMUETableManager')
    @patch('ecc.tasks.import_dag.import_data.create_postgres_hook')
    @patch('ecc.tasks.import_dag.import_data.ECCSourceHook')
    def test_creates_table_if_not_exists(self, MockECC, mock_pg, MockMgr, MockInserter, mock_batch_size):
        """AMUETableManager.manage_table est appelé pour créer la table si absente."""
        from ecc.tasks.import_dag.import_data import import_ecc_data

        mock_pg.return_value.get_first.return_value = (False,)  # table n'existe pas
        MockECC.return_value.execute_query.return_value = (['lifnr'], iter([]))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'

        import_ecc_data.function(_make_table_config())

        MockMgr.return_value.manage_table.assert_called_once()
