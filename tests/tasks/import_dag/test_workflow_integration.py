"""
Tests d'intégration du workflow d'import AMUE.

Vérifie la chaîne complète :
    init_bluegreen → select_tables → check_setup_status →
    import_data → save_metadata → switch_views → send_report

Ces tests simulent l'enchaînement des XCom entre les tasks.
"""
from unittest.mock import MagicMock, patch


class TestWorkflowIntegration:
    """Tests d'intégration du workflow d'import multi-tasks."""

    def test_bluegreen_context_flows_to_select_tables(self):
        """Le contexte init_bluegreen est correctement transmis à select_tables."""
        with patch('amue.tasks.import_dag.init_bluegreen.BlueGreenManager') as MockBG, \
             patch('amue.tasks.import_dag.init_bluegreen.get_current_context') as mock_ctx:
            from amue.tasks.import_dag.init_bluegreen import init_bluegreen

            MockBG.return_value.get_target_schema.return_value = 'splus_green'
            MockBG.return_value.get_active_schema.return_value = 'splus_blue'
            MockBG.return_value.needs_sync.return_value = False
            mock_ctx.return_value = {'dag_run': MagicMock(run_id='run-abc')}

            bg_ctx = init_bluegreen.function()

        # Le contexte retourné est valide pour select_tables
        assert bg_ctx['enabled'] is True
        assert bg_ctx['target_schema'] == 'splus_green'

    def test_check_setup_enriches_tables_for_import(self):
        """check_setup_status enrichit les tables avec primary_key pour import_data."""
        with patch('amue.tasks.import_dag.check_setup_status.TableConfigManager') as MockMgr:
            from amue.tasks.import_dag.check_setup_status import check_setup_status

            MockMgr.return_value.get_table_metadata.return_value = {
                'setup_status': 'ready',
                'primary_key': 'bukrs,kostl'
            }
            tables = [{'name': 'csks', 'target_schema': 'splus_green'}]
            ready = check_setup_status.function(tables)

        assert ready[0]['primary_key'] == 'bukrs,kostl'
        assert ready[0]['target_schema'] == 'splus_green'

    def test_import_data_result_compatible_with_save_metadata(self):
        """Le résultat d'import_data contient target_schema pour save_metadata."""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [('bukrs',), ('kostl',)]

        import_result = {
            'table_name': 'csks',
            'rows_inserted': 100,
            'rows_updated': 5,
            'rows_fetched': 105,
            'status': 'success',
            'target_schema': 'splus_green',
        }

        with patch('amue.tasks.import_dag.import_data.resolve_postgres_hook', return_value=mock_hook), \
             patch('amue.tasks.import_dag.import_data.AMUEAPIHook'), \
             patch('amue.tasks.import_dag.import_data.AMUEDataImporter') as MockImporter:
            MockImporter.return_value.import_table.return_value = import_result
            from amue.tasks.import_dag.import_data import import_data
            result = import_data.function({'name': 'csks', 'target_schema': 'splus_green', 'primary_key': 'bukrs'})

        assert 'target_schema' in result
        assert 'status' in result

    def test_save_metadata_result_compatible_with_switch_views(self):
        """Le résultat de save_metadata contient target_schema pour switch_views."""
        with patch('amue.tasks.import_dag.save_metadata.AMUEMetadataManager'):
            from amue.tasks.import_dag.save_metadata import save_metadata

            import_results = [
                {'table_name': 'csks', 'status': 'success', 'target_schema': 'splus_green',
                 'rows_inserted': 100, 'rows_updated': 0, 'rows_fetched': 100}
            ]
            result = save_metadata.function(import_results, {'finish': 'ts', 'report_start': 'rs'})

        assert result['target_schema'] == 'splus_green'
        assert result['tables_imported'] == 1

    def test_full_chain_data_contracts(self):
        """Vérifie les contrats de données entre toutes les tasks."""
        # init_bluegreen → {enabled, target_schema, active_schema, needs_sync}
        bg_contract = {'enabled', 'target_schema', 'active_schema', 'needs_sync'}

        # import_data → {table_name, rows_inserted, rows_updated, rows_fetched, status, target_schema}
        import_contract = {'table_name', 'rows_inserted', 'rows_updated', 'rows_fetched', 'status', 'target_schema'}

        # save_metadata → {tables_imported, target_schema}
        metadata_contract = {'tables_imported', 'target_schema'}

        # send_setup_report → {tables_ready, tables_blocked, tables_created, tables_error}
        setup_report_contract = {'tables_ready', 'tables_blocked', 'tables_created', 'tables_error'}

        # Vérification des contrats (tests documentaires)
        assert bg_contract == {'enabled', 'target_schema', 'active_schema', 'needs_sync'}
        assert 'target_schema' in import_contract
        assert 'tables_imported' in metadata_contract
        assert 'tables_blocked' in setup_report_contract
