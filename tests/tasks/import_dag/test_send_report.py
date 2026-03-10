"""Tests unitaires pour la task send_report."""
from unittest.mock import MagicMock, patch


class TestSendReport:
    """Tests pour la task d'envoi du rapport d'exécution AMUE."""

    def _make_import_result(self, table_name='csks', status='success'):
        return {
            'table_name': table_name,
            'rows_inserted': 1500,
            'rows_updated': 50,
            'rows_fetched': 1550,
            'status': status,
            'target_schema': 'splus_green',
        }

    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_calls_generate_and_send(self, MockGen):
        """Appelle generate_and_send sur le générateur de rapport."""
        from amue.tasks.import_dag.send_report import send_report

        MockGen.return_value.generate_and_send.return_value = {'sent': True, 'recipients': []}

        results = [self._make_import_result()]
        polling = {'finish': '2026-03-09T10:00:00', 'attempts': 3}
        switch = {'switched': True}

        result = send_report.function(results, switch, polling)

        MockGen.return_value.generate_and_send.assert_called_once_with(results, polling)
        assert result == {'sent': True, 'recipients': []}

    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_handles_none_polling_result(self, MockGen):
        """Fonctionne avec polling_result=None (défaut: {})."""
        from amue.tasks.import_dag.send_report import send_report

        MockGen.return_value.generate_and_send.return_value = {'sent': False}

        results = [self._make_import_result()]
        result = send_report.function(results, {}, None)

        call_args = MockGen.return_value.generate_and_send.call_args
        # polling_result doit être {} ou None → generate_and_send doit être appelé
        MockGen.return_value.generate_and_send.assert_called_once()

    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_returns_generator_result(self, MockGen):
        """Retourne directement le résultat du générateur."""
        from amue.tasks.import_dag.send_report import send_report

        expected = {'sent': True, 'recipients': ['admin@example.com'], 'report_id': 'rep-001'}
        MockGen.return_value.generate_and_send.return_value = expected

        result = send_report.function([], {}, {})

        assert result == expected
