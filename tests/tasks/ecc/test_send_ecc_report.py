"""Tests unitaires pour la task send_ecc_report."""
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


class TestSendEccReport:
    """Tests pour la task d'envoi du rapport ECC."""

    @patch('ecc.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_returns_report_with_ecc_summary(self, MockGen):
        """Le résultat contient ecc_summary avec les métriques agrégées."""
        from ecc.tasks.import_dag.send_report import send_ecc_report

        MockGen.return_value.generate_and_send.return_value = {'sent': True}

        results = [_make_result('lfa1', rows_fetched=100), _make_result('csks', rows_fetched=50)]
        result = send_ecc_report.function(results)

        assert 'ecc_summary' in result
        assert result['ecc_summary']['tables_processed'] == 2
        assert result['ecc_summary']['total_fetched'] == 150

    @patch('ecc.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_counts_only_success_tables(self, MockGen):
        """tables_success compte uniquement les tables en succès."""
        from ecc.tasks.import_dag.send_report import send_ecc_report

        MockGen.return_value.generate_and_send.return_value = {}

        results = [_make_result(status='success'), _make_result(status='error')]
        result = send_ecc_report.function(results)

        assert result['ecc_summary']['tables_success'] == 1

    @patch('ecc.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_aggregates_all_row_counts(self, MockGen):
        """Agrège correctement rows_inserted, rows_updated, rows_skipped."""
        from ecc.tasks.import_dag.send_report import send_ecc_report

        MockGen.return_value.generate_and_send.return_value = {}

        results = [
            {**_make_result(), 'rows_inserted': 80, 'rows_updated': 15, 'rows_skipped': 5, 'rows_fetched': 100},
            {**_make_result(), 'rows_inserted': 50, 'rows_updated': 0, 'rows_skipped': 0, 'rows_fetched': 50},
        ]
        result = send_ecc_report.function(results)

        assert result['ecc_summary']['total_inserted'] == 130
        assert result['ecc_summary']['total_updated'] == 15
        assert result['ecc_summary']['total_skipped'] == 5
        assert result['ecc_summary']['total_fetched'] == 150

    @patch('ecc.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_calls_generate_with_title_ecc(self, MockGen):
        """generate_and_send est appelé avec title='RAPPORT IMPORT ECC'."""
        from ecc.tasks.import_dag.send_report import send_ecc_report

        MockGen.return_value.generate_and_send.return_value = {}

        send_ecc_report.function([_make_result()])

        call_kwargs = MockGen.return_value.generate_and_send.call_args[1]
        assert call_kwargs.get('title') == 'RAPPORT IMPORT ECC'

    @patch('ecc.tasks.import_dag.send_report.AMUEReportGenerator')
    def test_empty_results_handled(self, MockGen):
        """Fonctionne avec une liste de résultats vide."""
        from ecc.tasks.import_dag.send_report import send_ecc_report

        MockGen.return_value.generate_and_send.return_value = {}

        result = send_ecc_report.function([])

        assert result['ecc_summary']['tables_processed'] == 0
        assert result['ecc_summary']['total_fetched'] == 0
