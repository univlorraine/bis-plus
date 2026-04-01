"""Tests unitaires pour AMUEReportGenerator."""
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


def _make_generator():
    from amue.notifications.report_generator import AMUEReportGenerator
    with patch('amue.notifications.report_generator.NotificationService'):
        gen = AMUEReportGenerator()
    gen.notification_service = MagicMock()
    return gen


def _sample_import_results(n=3):
    return [
        {
            'table_name': f'TABLE{i}',
            'rows_fetched': 100 * i,
            'rows_inserted': 80 * i,
            'rows_updated': 10 * i,
            'import_type': 'full',
            'status': 'success',
        }
        for i in range(1, n + 1)
    ]


def _sample_polling_result(start_offset_seconds=300):
    start = (datetime.now() - timedelta(seconds=start_offset_seconds)).isoformat()
    return {'start_time': start, 'attempts': 3, 'total_wait_minutes': 5.0}


class TestGenerateReport:

    def test_returns_dict_with_required_keys(self):
        gen = _make_generator()
        with patch.object(gen, '_save_report'):
            report = gen.generate_report(_sample_import_results(), _sample_polling_result())
        for key in ('status', 'total_tables', 'total_fetched', 'total_inserted', 'tables_detail'):
            assert key in report

    def test_status_is_success(self):
        gen = _make_generator()
        with patch.object(gen, '_save_report'):
            report = gen.generate_report(_sample_import_results(), _sample_polling_result())
        assert report['status'] == 'success'

    def test_aggregates_totals_correctly(self):
        gen = _make_generator()
        results = [
            {'table_name': 'A', 'rows_fetched': 100, 'rows_inserted': 80, 'rows_updated': 5, 'status': 'success'},
            {'table_name': 'B', 'rows_fetched': 200, 'rows_inserted': 150, 'rows_updated': 10, 'status': 'success'},
        ]
        with patch.object(gen, '_save_report'):
            report = gen.generate_report(results, {})
        assert report['total_fetched'] == 300
        assert report['total_inserted'] == 230
        assert report['total_tables'] == 2

    def test_tables_with_zero_rows_counted_as_skipped(self):
        gen = _make_generator()
        results = [
            {'table_name': 'A', 'rows_fetched': 100, 'rows_inserted': 80, 'rows_updated': 0, 'status': 'success'},
            {'table_name': 'B', 'rows_fetched': 0, 'rows_inserted': 0, 'rows_updated': 0, 'status': 'success'},
        ]
        with patch.object(gen, '_save_report'):
            report = gen.generate_report(results, {})
        assert report['tables_skipped'] == 1
        assert report['total_tables'] == 1

    def test_empty_results_returns_zero_totals(self):
        gen = _make_generator()
        with patch.object(gen, '_save_report'):
            report = gen.generate_report([], {})
        assert report['total_tables'] == 0
        assert report['total_fetched'] == 0
        assert report['tables_detail'] == []

    def test_tables_detail_contains_truncated_fingerprint(self):
        gen = _make_generator()
        results = [{
            'table_name': 'T',
            'rows_fetched': 1,
            'rows_inserted': 1,
            'rows_updated': 0,
            'status': 'success',
            'fingerprint_API': 'abcdefghijklmnopqrstuvwxyz',
        }]
        with patch.object(gen, '_save_report'):
            report = gen.generate_report(results, {})
        assert report['tables_detail'][0]['fingerprint_API'].endswith('...')


class TestCalculateDuration:

    def test_calculates_duration_from_start_time(self):
        gen = _make_generator()
        start = (datetime.now() - timedelta(minutes=10, seconds=30)).isoformat()
        result = gen._calculate_duration({'start_time': start})
        assert 'm' in result  # should contain minutes

    def test_falls_back_to_wait_minutes_when_no_start(self):
        gen = _make_generator()
        result = gen._calculate_duration({'total_wait_minutes': 5.5})
        assert '5.5' in result

    def test_returns_na_when_no_data(self):
        gen = _make_generator()
        result = gen._calculate_duration({})
        assert result == 'N/A'

    def test_handles_invalid_start_time(self):
        gen = _make_generator()
        result = gen._calculate_duration({'start_time': 'not-a-date'})
        assert result == 'N/A'


class TestFormatDuration:

    def test_seconds_only(self):
        gen = _make_generator()
        assert gen._format_duration(45) == '45s'

    def test_minutes_and_seconds(self):
        gen = _make_generator()
        assert gen._format_duration(125) == '2m 5s'

    def test_hours_and_minutes(self):
        gen = _make_generator()
        assert gen._format_duration(3661) == '1h 1m'


class TestSendNotification:

    def test_calls_notify_success(self):
        gen = _make_generator()
        gen.notification_service.notify_success.return_value = True
        report = {
            'execution_date': datetime.now().isoformat(),
            'duration': '5m',
            'tables_detail': [],
            'total_inserted': 0,
            'total_fetched': 0,
        }
        gen.send_notification(report)
        gen.notification_service.notify_success.assert_called_once()

    def test_handles_notify_failure_gracefully(self):
        gen = _make_generator()
        gen.notification_service.notify_success.return_value = False
        gen.send_notification({'execution_date': '', 'duration': '', 'tables_detail': [],
                               'total_inserted': 0, 'total_fetched': 0})
        # should not raise


class TestGenerateAndSend:

    def test_calls_generate_then_send(self):
        gen = _make_generator()
        gen.notification_service.notify_success.return_value = True
        with patch.object(gen, '_save_report'), \
             patch.object(gen, 'send_notification') as mock_send:
            report = gen.generate_and_send(_sample_import_results(), _sample_polling_result())
        mock_send.assert_called_once_with(report, dag_id='amue_multi_table_import')
        assert report['status'] == 'success'
