"""Tests unitaires pour la task send_report ECC."""
from unittest.mock import MagicMock, patch


def _make_result(table_name='lfa1', status='success', rows_fetched=100):
    return {
        'table_name': table_name,
        'rows_fetched': rows_fetched,
        'rows_inserted': rows_fetched,
        'rows_updated': 0,
        'rows_skipped': 0,
        'status': status,
        'target_schema': 'splus_blue',
    }


def _stub_context():
    return {'dag_run': MagicMock(start_date=None)}


class TestSendReport:
    """Tests pour la task d'envoi du rapport ECC."""

    @patch('ecc.tasks.import_dag.send_report.get_current_context', side_effect=_stub_context)
    @patch('ecc.infrastructure.notifications.ecc_notifier.ECCNotificationService')
    def test_returns_report_with_ecc_summary(self, MockSvc, mock_ctx):
        from ecc.tasks.import_dag.send_report import send_report

        results = [_make_result('lfa1', rows_fetched=100), _make_result('csks', rows_fetched=50)]
        result = send_report.function(results)

        assert 'ecc_summary' in result
        assert result['ecc_summary']['tables_processed'] == 2
        assert result['ecc_summary']['total_fetched'] == 150

    @patch('ecc.tasks.import_dag.send_report.get_current_context', side_effect=_stub_context)
    @patch('ecc.infrastructure.notifications.ecc_notifier.ECCNotificationService')
    def test_counts_only_success_tables(self, MockSvc, mock_ctx):
        from ecc.tasks.import_dag.send_report import send_report

        results = [_make_result(status='success'), _make_result(status='error')]
        result = send_report.function(results)

        assert result['ecc_summary']['tables_success'] == 1

    @patch('ecc.tasks.import_dag.send_report.get_current_context', side_effect=_stub_context)
    @patch('ecc.infrastructure.notifications.ecc_notifier.ECCNotificationService')
    def test_aggregates_all_row_counts(self, MockSvc, mock_ctx):
        from ecc.tasks.import_dag.send_report import send_report

        results = [
            {**_make_result(), 'rows_inserted': 80, 'rows_updated': 15, 'rows_skipped': 5, 'rows_fetched': 100},
            {**_make_result(), 'rows_inserted': 50, 'rows_updated': 0, 'rows_skipped': 0, 'rows_fetched': 50},
        ]
        result = send_report.function(results)

        assert result['ecc_summary']['total_inserted'] == 130
        assert result['ecc_summary']['total_updated'] == 15
        assert result['ecc_summary']['total_skipped'] == 5
        assert result['ecc_summary']['total_fetched'] == 150

    @patch('ecc.tasks.import_dag.send_report.get_current_context', side_effect=_stub_context)
    @patch('ecc.infrastructure.notifications.ecc_notifier.ECCNotificationService')
    def test_calls_notify_success_with_ecc_dag_id(self, MockSvc, mock_ctx):
        """notify_success est appelé avec dag_id='ecc_multi_table_import'."""
        from ecc.tasks.import_dag.send_report import send_report

        send_report.function([_make_result()])

        MockSvc.return_value.notify_success.assert_called_once()
        call_arg = MockSvc.return_value.notify_success.call_args[0][0]
        assert call_arg['dag_id'] == 'ecc_multi_table_import'
        assert call_arg['title'] == 'Import ECC Réussi'

    @patch('ecc.tasks.import_dag.send_report.get_current_context', side_effect=_stub_context)
    @patch('ecc.infrastructure.notifications.ecc_notifier.ECCNotificationService')
    def test_empty_results_handled(self, MockSvc, mock_ctx):
        from ecc.tasks.import_dag.send_report import send_report

        result = send_report.function([])

        assert result['ecc_summary']['tables_processed'] == 0
        assert result['ecc_summary']['total_fetched'] == 0


class TestSummarizeImportResults:
    """Tests unitaires pour le helper commun."""

    def test_empty_list(self):
        from common.tasks.import_summary import summarize_import_results

        s = summarize_import_results([])
        assert s == {
            'tables_processed': 0,
            'tables_success': 0,
            'total_fetched': 0,
            'total_inserted': 0,
            'total_updated': 0,
            'total_skipped': 0,
        }

    def test_aggregation(self):
        from common.tasks.import_summary import summarize_import_results

        results = [
            {'status': 'success', 'rows_fetched': 100, 'rows_inserted': 80, 'rows_updated': 15, 'rows_skipped': 5},
            {'status': 'error', 'rows_fetched': 0, 'rows_inserted': 0, 'rows_updated': 0, 'rows_skipped': 0},
        ]
        s = summarize_import_results(results)
        assert s['tables_processed'] == 2
        assert s['tables_success'] == 1
        assert s['total_fetched'] == 100
        assert s['total_inserted'] == 80
