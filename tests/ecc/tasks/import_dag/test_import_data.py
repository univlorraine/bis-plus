"""Tests unitaires pour la task import_data ECC."""
from unittest.mock import MagicMock, patch


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


class TestImportData:
    """Tests pour la task d'import Oracle → PostgreSQL."""

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=100)
    @patch('ecc.tasks.import_dag.import_data.ECCDataImporter')
    def test_delegates_to_importer(self, MockImporter, mock_batch_size):
        """La @task instancie ECCDataImporter et délègue à import_table()."""
        from ecc.tasks.import_dag.import_data import import_data

        MockImporter.return_value.import_table.return_value = {
            'table_name': 'lfa1',
            'rows_fetched': 0,
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_skipped': 0,
            'status': 'success',
            'target_schema': 'splus_blue',
            'import_type': 'full',
        }

        result = import_data.function(_make_table_config())

        MockImporter.assert_called_once_with(
            target_schema='splus_blue',
            source='ecc',
            protected_source='sifac_plus',
        )
        MockImporter.return_value.import_table.assert_called_once_with(
            table_name='lfa1',
            ecc_query='SELECT * FROM LFA1',
            primary_keys=['lifnr'],
            batch_size=100,
        )
        assert result['status'] == 'success'
        assert result['target_schema'] == 'splus_blue'

    @patch('ecc.tasks.import_dag.import_data.get_ecc_batch_size', return_value=5000)
    @patch('ecc.tasks.import_dag.import_data.ECCDataImporter')
    def test_uses_default_source_if_missing(self, MockImporter, mock_batch_size):
        """Si source/protected_source absents, valeurs par défaut appliquées."""
        from ecc.tasks.import_dag.import_data import import_data

        MockImporter.return_value.import_table.return_value = {
            'table_name': 'lfa1', 'status': 'success', 'rows_fetched': 0,
            'rows_inserted': 0, 'rows_updated': 0, 'rows_skipped': 0,
            'target_schema': 'splus_blue', 'import_type': 'full',
        }

        config = _make_table_config()
        config.pop('source')
        config.pop('protected_source')

        import_data.function(config)

        MockImporter.assert_called_once_with(
            target_schema='splus_blue',
            source='ecc',
            protected_source='sifac_plus',
        )


class TestECCDataImporter:
    """Tests pour la classe ECCDataImporter."""

    @patch('ecc.application.ecc_data_importer.create_postgres_hook')
    def test_init_creates_pg_hook_with_target_schema(self, mock_create_hook):
        from ecc.application.ecc_data_importer import ECCDataImporter

        ECCDataImporter(target_schema='splus_blue', source='ecc', protected_source='sifac_plus')
        mock_create_hook.assert_called_once_with(bluegreen_schema='splus_blue')

    @patch('ecc.application.ecc_data_importer.BatchUpserter')
    @patch('ecc.application.ecc_data_importer.ECCSourceHook')
    @patch('ecc.application.ecc_data_importer.create_postgres_hook')
    def test_empty_oracle_result_returns_zero_rows(self, mock_pg, MockECC, MockInserter):
        from ecc.application.ecc_data_importer import ECCDataImporter

        mock_pg.return_value.get_first.return_value = (True,)
        MockECC.return_value.execute_query.return_value = (['lifnr'], iter([]))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'

        importer = ECCDataImporter(target_schema='splus_blue', source='ecc', protected_source='sifac_plus')
        result = importer.import_table('lfa1', 'SELECT * FROM LFA1', ['lifnr'], batch_size=100)

        assert result['rows_fetched'] == 0
        assert result['status'] == 'success'

    @patch('ecc.application.ecc_data_importer.BatchUpserter')
    @patch('ecc.application.ecc_data_importer.ECCSourceHook')
    @patch('ecc.application.ecc_data_importer.create_postgres_hook')
    def test_rows_fetched_counts_all_rows(self, mock_pg, MockECC, MockInserter):
        from ecc.application.ecc_data_importer import ECCDataImporter

        rows = [('LF001',), ('LF002',), ('LF003',)]
        mock_pg.return_value.get_first.return_value = (True,)
        MockECC.return_value.execute_query.return_value = (['lifnr'], iter(rows))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'
        MockInserter.return_value.execute_batch.return_value = {
            'rows_inserted': 2, 'rows_updated': 0, 'rows_affected': 2, 'batch_size': 2
        }

        importer = ECCDataImporter(target_schema='splus_blue', source='ecc', protected_source='sifac_plus')
        result = importer.import_table('lfa1', 'SELECT * FROM LFA1', ['lifnr'], batch_size=2)

        assert result['rows_fetched'] == 3

    @patch('ecc.application.ecc_data_importer.BatchUpserter')
    @patch('ecc.application.ecc_data_importer.ECCSourceHook')
    @patch('ecc.application.ecc_data_importer.create_postgres_hook')
    def test_creates_table_if_not_exists(self, mock_pg, MockECC, MockInserter):
        """run() est appelé pour CREATE TABLE quand la table est absente."""
        from ecc.application.ecc_data_importer import ECCDataImporter

        mock_pg.return_value.get_first.return_value = (False,)  # table n'existe pas
        MockECC.return_value.execute_query.return_value = (['lifnr', 'name1'], iter([]))
        MockInserter.return_value.get_connection.return_value = MagicMock()
        MockInserter.return_value.build_insert_sql_for_values.return_value = 'INSERT ...'

        importer = ECCDataImporter(target_schema='splus_blue', source='ecc', protected_source='sifac_plus')
        importer.import_table('lfa1', 'SELECT * FROM LFA1', ['lifnr'], batch_size=100)

        # Vérifie qu'un CREATE TABLE a été émis
        run_calls = mock_pg.return_value.run.call_args_list
        assert len(run_calls) == 1
        ddl = run_calls[0][0][0]
        assert 'CREATE TABLE' in ddl
        assert '"splus_blue"."lfa1"' in ddl
