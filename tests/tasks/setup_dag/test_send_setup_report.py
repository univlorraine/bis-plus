"""Tests unitaires pour la task send_setup_report."""
from unittest.mock import MagicMock, patch


def _r(table_name, setup_status='ready', status='success', created=False, error=None):
    """Helper : construit un résultat de setup_table."""
    r = {'table_name': table_name, 'setup_status': setup_status, 'status': status, 'created': created}
    if error:
        r['error'] = error
    return r


class TestSendSetupReport:
    """Tests pour la task de rapport du setup des tables."""

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_counts_tables_by_status(self, MockNotif):
        """Compte correctement les tables par statut."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        results = [
            _r('csks', 'ready', created=False),
            _r('lfa1', 'ready', created=True),
            _r('t001', 'blocked', 'error'),
            _r('t002', 'ready', 'error'),
        ]
        result = send_setup_report.function(results)

        assert result['tables_ready'] == 2   # csks + lfa1
        assert result['tables_blocked'] == 1  # t001
        assert result['tables_created'] == 1  # lfa1 created=True
        assert result['tables_error'] == 1    # t002 (status=error, not blocked)

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_sends_notification_on_blocked_tables(self, MockNotif):
        """Envoie une notification si des tables sont bloquées."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        results = [_r('csks', 'blocked', 'error', error='structure modifiée')]
        send_setup_report.function(results)

        MockNotif.return_value.notify_setup_error.assert_called_once()

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_no_notification_when_all_ready(self, MockNotif):
        """Pas de notification si toutes les tables sont prêtes."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        results = [_r('csks', 'ready'), _r('lfa1', 'ready', created=True)]
        send_setup_report.function(results)

        MockNotif.return_value.notify_setup_error.assert_not_called()

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_notification_failure_does_not_crash(self, MockNotif):
        """Si la notification échoue, la task ne plante pas."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        MockNotif.return_value.notify_setup_error.side_effect = Exception("SMTP error")
        results = [_r('csks', 'blocked', 'error')]

        # Ne doit pas lever d'exception
        result = send_setup_report.function(results)
        assert result['tables_blocked'] == 1

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_empty_results_returns_zeros(self, MockNotif):
        """Une liste vide retourne des compteurs à 0."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        result = send_setup_report.function([])

        assert result == {
            'tables_ready': 0,
            'tables_blocked': 0,
            'tables_created': 0,
            'tables_error': 0,
        }

    @patch('amue.tasks.setup_dag.send_setup_report.NotificationService')
    def test_sends_notification_on_error_tables(self, MockNotif):
        """Envoie une notification si des tables sont en erreur (hors blocked)."""
        from amue.tasks.setup_dag.send_setup_report import send_setup_report

        results = [_r('csks', 'ready', 'error', error='timeout')]
        send_setup_report.function(results)

        MockNotif.return_value.notify_setup_error.assert_called_once()
