"""Tests unitaires pour les tasks select_tables."""
from unittest.mock import MagicMock, patch


class TestSelectTables:
    """Tests pour la task select_tables."""

    def _run_select(self, polling_result, tables, bluegreen_ctx, target_schema='splus_green'):
        """Helper : exécute select_tables avec mocks actifs."""
        from amue.tasks.import_dag.polling import select_tables

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = lambda task_ids=None, key=None: (
            polling_result if task_ids == 'wait_for_api' and key is None else None
        )

        mock_filter = MagicMock()
        mock_filter.filter_tables.return_value = tables

        with patch('amue.tasks.import_dag.polling.get_current_context') as mock_ctx, \
             patch('amue.tasks.import_dag.polling.AMUETableFilter', return_value=mock_filter):
            mock_ctx.return_value = {'ti': mock_ti}
            return select_tables.function(bluegreen_ctx)

    def test_returns_tables_with_target_schema(self):
        """Chaque table reçoit target_schema depuis bluegreen_ctx."""
        tables = [{'name': 'csks'}, {'name': 'lfa1'}]
        polling = {'tables_status': {'CSKS': {}, 'LFA1': {}}}
        ctx = {'enabled': True, 'target_schema': 'splus_green'}

        result = self._run_select(polling, tables, ctx)

        assert len(result) == 2
        for t in result:
            assert t['target_schema'] == 'splus_green'

    def test_returns_empty_when_no_tables(self):
        """Retourne [] quand aucune table à importer."""
        polling = {'tables_status': {}}
        ctx = {'enabled': True, 'target_schema': 'splus_green'}

        result = self._run_select(polling, [], ctx)

        assert result == []

    @patch('amue.tasks.import_dag.polling.AMUEStatusChecker')
    @patch('amue.tasks.import_dag.polling.AMUEAPIHook')
    def test_fallback_api_call_when_xcom_empty(self, MockAPI, MockChecker):
        """Appelle l'API directement si XCom vide (tables_status absent)."""
        from amue.tasks.import_dag.polling import select_tables

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {}  # polling_result sans tables_status

        mock_filter = MagicMock()
        mock_filter.filter_tables.return_value = []

        MockChecker.return_value.get_current_status.return_value = {}

        with patch('amue.tasks.import_dag.polling.get_current_context') as mock_ctx, \
             patch('amue.tasks.import_dag.polling.AMUETableFilter', return_value=mock_filter):
            mock_ctx.return_value = {'ti': mock_ti}
            select_tables.function({'enabled': True, 'target_schema': 'splus_green'})

        MockChecker.return_value.get_current_status.assert_called_once()

    def test_no_target_schema_when_bluegreen_disabled(self):
        """target_schema=None si bluegreen désactivé."""
        tables = [{'name': 'csks'}]
        polling = {'tables_status': {'CSKS': {}}}
        ctx = {'enabled': False, 'target_schema': None}

        result = self._run_select(polling, tables, ctx)

        assert result[0]['target_schema'] is None
