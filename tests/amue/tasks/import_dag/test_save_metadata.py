"""Tests unitaires pour la task save_metadata."""
from unittest.mock import MagicMock, patch


class TestSaveMetadata:
    """Tests pour la task de sauvegarde des métadonnées AMUE."""

    def _make_import_result(self, table_name='csks', target_schema='splus_green', **kwargs):
        base = {
            'table_name': table_name,
            'rows_inserted': 100,
            'rows_updated': 10,
            'rows_fetched': 110,
            'import_type': 'delta',
            'status': 'success',
            'target_schema': target_schema,
        }
        base.update(kwargs)
        return base

    @patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager')
    def test_returns_tables_imported_and_schema(self, MockMgr):
        """Retourne tables_imported et target_schema."""
        from amue.tasks.import_dag.save_metadata import save_metadata

        results = [self._make_import_result('csks'), self._make_import_result('lfa1')]
        polling = {'finish': '2026-03-09T10:00:00', 'report_start': '2026-03-09T08:00:00'}

        result = save_metadata.function(results, polling)

        assert result['tables_imported'] == 2
        assert result['target_schema'] == 'splus_green'

    @patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager')
    def test_calls_update_metadata(self, MockMgr):
        """AMUEMetadataManager.update_metadata est appelé."""
        from amue.tasks.import_dag.save_metadata import save_metadata

        results = [self._make_import_result()]
        polling = {'finish': '2026-03-09T10:00:00', 'report_start': ''}

        save_metadata.function(results, polling)

        MockMgr.return_value.update_metadata.assert_called_once()

    @patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager')
    def test_handles_empty_import_results(self, MockMgr):
        """Fonctionne avec une liste d'imports vide."""
        from amue.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function([], {'finish': '', 'report_start': ''})

        assert result['tables_imported'] == 0
        assert result['target_schema'] is None

    @patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager')
    def test_handles_none_polling_result(self, MockMgr):
        """Fonctionne avec polling_result=None (XCom vide)."""
        from amue.tasks.import_dag.save_metadata import save_metadata

        results = [self._make_import_result()]
        result = save_metadata.function(results, None)

        assert result['tables_imported'] == 1
        call_kwargs = MockMgr.return_value.update_metadata.call_args
        assert call_kwargs[1]['finish_timestamp'] == ''

    @patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager')
    def test_passes_finish_timestamp_to_manager(self, MockMgr):
        """Le finish timestamp est transmis à update_metadata."""
        from amue.tasks.import_dag.save_metadata import save_metadata

        results = [self._make_import_result()]
        polling = {'finish': '2026-03-09T10:18:22', 'report_start': '2026-03-09T08:00:00'}

        save_metadata.function(results, polling)

        call_kwargs = MockMgr.return_value.update_metadata.call_args
        assert call_kwargs[1]['finish_timestamp'] == '2026-03-09T10:18:22'
        assert call_kwargs[1]['report_start'] == '2026-03-09T08:00:00'
