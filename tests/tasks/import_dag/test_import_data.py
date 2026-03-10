"""Tests unitaires pour la task import_data."""
import pytest
from unittest.mock import MagicMock, patch


class TestImportData:
    """Tests pour la task d'import des données AMUE."""

    def _make_hook(self, columns=None):
        """Helper : hook PostgreSQL mocké retournant des colonnes."""
        hook = MagicMock()
        hook.get_records.return_value = [(c,) for c in (columns or ['bukrs', 'kostl', 'datab'])]
        return hook

    def _make_importer_result(self, **kwargs):
        """Helper : résultat d'import standard."""
        base = {
            'table_name': 'csks',
            'rows_inserted': 1500,
            'rows_updated': 50,
            'rows_fetched': 1550,
            'import_type': 'differential',
            'fingerprint_API': 'abc123',
            'fingerprint_UL': 'def456',
            'status': 'success',
            'correlation_id': 'import-test01',
            'batch_count': 1,
            'total_duration_seconds': 2.5,
            'avg_batch_duration': 2.5,
        }
        base.update(kwargs)
        return base

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_import_success_returns_expected_keys(self, mock_hook_fn, MockImporter, MockAPI):
        """La task retourne les clés attendues avec target_schema."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook()
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs,kostl'}
        result = import_data.function(table_info)

        assert result['status'] == 'success'
        assert result['target_schema'] == 'splus_green'
        assert result['table_name'] == 'csks'

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_target_schema_injected_in_result(self, mock_hook_fn, MockImporter, MockAPI):
        """target_schema est ajouté au résultat même si absent du retour importer."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook()
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': 'splus_blue', 'primary_key': 'bukrs'}
        result = import_data.function(table_info)

        assert result['target_schema'] == 'splus_blue'

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_raises_when_no_columns_found(self, mock_hook_fn, MockAPI):
        """Lève une exception si aucune colonne trouvée dans information_schema."""
        from amue.tasks.import_dag.import_data import import_data

        hook = MagicMock()
        hook.get_records.return_value = []
        mock_hook_fn.return_value = hook

        table_info = {'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs'}
        with pytest.raises(Exception, match="Aucune colonne"):
            import_data.function(table_info)

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_primary_keys_parsed_from_csv(self, mock_hook_fn, MockImporter, MockAPI):
        """Les clés primaires CSV sont parsées en liste."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook()
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs, kostl'}
        import_data.function(table_info)

        call_kwargs = MockImporter.return_value.import_table.call_args
        primary_keys = call_kwargs[1]['primary_keys']
        assert primary_keys == ['bukrs', 'kostl']

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_importer_called_with_table_config(self, mock_hook_fn, MockImporter, MockAPI):
        """AMUEDataImporter.import_table est appelé avec les bons paramètres."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook(['col1', 'col2'])
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs'}
        import_data.function(table_info)

        MockImporter.return_value.import_table.assert_called_once()
        call_kwargs = MockImporter.return_value.import_table.call_args[1]
        assert call_kwargs['table_name'] == 'csks'
        assert 'col1' in call_kwargs['columns']
        assert 'col2' in call_kwargs['columns']

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_uses_bluegreen_hook_when_target_schema_provided(self, mock_hook_fn, MockImporter, MockAPI):
        """create_postgres_hook est appelé avec bluegreen_schema quand target_schema fourni."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook()
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs'}
        import_data.function(table_info)

        mock_hook_fn.assert_called_once_with(bluegreen_schema='splus_green')

    @patch('amue.tasks.import_dag.import_data.AMUEAPIHook')
    @patch('amue.tasks.import_dag.import_data.AMUEDataImporter')
    @patch('amue.tasks.import_dag.import_data.create_postgres_hook')
    def test_none_target_schema_handled(self, mock_hook_fn, MockImporter, MockAPI):
        """target_schema=None est géré (appel sans bluegreen_schema)."""
        from amue.tasks.import_dag.import_data import import_data

        mock_hook_fn.return_value = self._make_hook()
        MockImporter.return_value.import_table.return_value = self._make_importer_result()

        table_info = {'name': 'csks', 'target_schema': None, 'primary_key': 'bukrs'}
        result = import_data.function(table_info)

        assert result['target_schema'] is None
