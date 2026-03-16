"""Tests unitaires pour la task send_report."""
import pytest
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

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_calls_generate_and_send(self, MockGen, MockTCM):
        """Appelle generate_and_send sur le générateur de rapport."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        MockGen.return_value.generate_and_send.return_value = {'sent': True, 'recipients': []}

        results = [self._make_import_result()]
        polling = {'finish': '2026-03-09T10:00:00', 'attempts': 3}
        switch = {'switched': True}

        result = send_report.function(results, switch, polling)

        MockGen.return_value.generate_and_send.assert_called_once_with(results, polling)
        assert result == {'sent': True, 'recipients': []}

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_handles_none_polling_result(self, MockGen, MockTCM):
        """Fonctionne avec polling_result=None (défaut: {})."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        MockGen.return_value.generate_and_send.return_value = {'sent': False}

        results = [self._make_import_result()]
        result = send_report.function(results, {}, None)

        # polling_result doit être {} ou None → generate_and_send doit être appelé
        MockGen.return_value.generate_and_send.assert_called_once()

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_returns_generator_result(self, MockGen, MockTCM):
        """Retourne directement le résultat du générateur."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        expected = {'sent': True, 'recipients': ['admin@example.com'], 'report_id': 'rep-001'}
        MockGen.return_value.generate_and_send.return_value = expected

        result = send_report.function([], {}, {})

        assert result == expected

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_propagates_generator_exception(self, MockGen, MockTCM):
        """Propage l'exception si generate_and_send lève une erreur."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        MockGen.return_value.generate_and_send.side_effect = RuntimeError("Email service down")

        with pytest.raises(RuntimeError, match="Email service down"):
            send_report.function([self._make_import_result()], {}, {})

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_empty_import_results(self, MockGen, MockTCM):
        """Fonctionne avec une liste de résultats vide."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        MockGen.return_value.generate_and_send.return_value = {'sent': True, 'recipients': []}

        result = send_report.function([], {}, {})

        MockGen.return_value.generate_and_send.assert_called_once_with([], {})
        assert result['sent'] is True

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_switch_result_not_forwarded_to_generator(self, MockGen, MockTCM):
        """switch_result n'est pas transmis à generate_and_send (non utilisé)."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = []
        MockGen.return_value.generate_and_send.return_value = {'sent': True}
        polling = {'finish': '2026-03-09T10:00:00'}
        switch = {'switched': True, 'schema': 'splus_green'}

        send_report.function([], switch, polling)

        call_args = MockGen.return_value.generate_and_send.call_args
        # Seuls import_results et polling_result sont passés
        assert call_args == (([],  polling), {})

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_logs_error_for_blocked_tables(self, MockGen, MockTCM):
        """Log une erreur si des tables sont bloquées."""
        import logging
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = [
            {'name': 'csks', 'setup_status': 'blocked'},
            {'name': 'cskb', 'setup_status': 'ok'},
        ]
        MockGen.return_value.generate_and_send.return_value = {'sent': True}

        with patch('amue.tasks.import_dag.send_report.logger') as mock_logger:
            send_report.function([], {}, {})
            mock_logger.error.assert_called_once()
            assert 'csks' in mock_logger.error.call_args[0][0]

    @patch('amue.tasks.import_dag.send_report.TableConfigManager')
    @patch('amue.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_no_error_when_no_blocked_tables(self, MockGen, MockTCM):
        """Pas de log d'erreur si aucune table bloquée."""
        from amue.tasks.import_dag.send_report import send_report

        MockTCM.return_value.get_tables_config.return_value = [
            {'name': 'csks', 'setup_status': 'ok'},
        ]
        MockGen.return_value.generate_and_send.return_value = {'sent': True}

        with patch('amue.tasks.import_dag.send_report.logger') as mock_logger:
            send_report.function([], {}, {})
            mock_logger.error.assert_not_called()
