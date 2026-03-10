"""Tests unitaires pour StatusMonitor."""
from unittest.mock import MagicMock, patch, call


class TestStatusMonitorWatch:
    """Tests pour StatusMonitor.watch()"""

    def _run_n_iterations(self, checker, n_iterations=1):
        """
        Exécute watch() pour n_iterations en stoppant la deadline après.

        Remplace datetime.now pour simuler un temps figé puis expiré.
        """
        from amue.services.api.status_monitor import StatusMonitor
        from datetime import datetime, timedelta

        iteration = [0]

        def fake_now():
            iteration[0] += 1
            # Les 2 premiers appels (init + while) → dans la fenêtre
            # Ensuite → au-delà de la deadline
            if iteration[0] <= 1 + n_iterations * 2:
                return datetime(2026, 3, 9, 10, 0, 0)
            return datetime(2026, 3, 9, 15, 0, 0)

        monitor = StatusMonitor(checker, duration_hours=4, poll_interval_seconds=0)

        with patch('amue.services.api.status_monitor.datetime') as mock_dt, \
             patch('amue.services.api.status_monitor.time') as mock_time:
            mock_dt.now.side_effect = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            monitor.watch()

        return mock_time

    def test_first_response_is_always_logged(self):
        """Premier appel toujours loggé (previous_snapshot=None)."""
        checker = MagicMock()
        checker.fetch_full_status.return_value = {
            'status': 'ready', 'raw_response': {'state': 'ok'}
        }

        with patch('amue.services.api.status_monitor.logger') as mock_logger:
            self._run_n_iterations(checker, n_iterations=1)
            info_messages = [str(c) for c in mock_logger.info.call_args_list]
            assert any('CHANGEMENT' in m for m in info_messages)

    def test_api_error_logs_warning_and_continues(self):
        """Une erreur API log un warning mais n'arrête pas la boucle."""
        checker = MagicMock()
        checker.fetch_full_status.side_effect = [
            Exception("API down"),
            {'status': 'ready', 'raw_response': {'state': 'ok'}},
        ]

        self._run_n_iterations(checker, n_iterations=2)
        assert checker.fetch_full_status.call_count == 2

    def test_no_duplicate_log_for_same_response(self):
        """Réponse identique au précédent appel → pas de log CHANGEMENT."""
        same_response = {'status': 'ready', 'raw_response': {'state': 'ok'}}
        checker = MagicMock()
        checker.fetch_full_status.side_effect = [same_response, same_response]

        with patch('amue.services.api.status_monitor.logger') as mock_logger:
            self._run_n_iterations(checker, n_iterations=2)
            info_messages = [str(c) for c in mock_logger.info.call_args_list]
            changement_calls = [m for m in info_messages if 'CHANGEMENT' in m]
            # Seul le premier appel doit loguer CHANGEMENT
            assert len(changement_calls) == 1

    def test_sleep_called_with_poll_interval(self):
        """time.sleep est appelé avec l'intervalle de polling."""
        from amue.services.api.status_monitor import StatusMonitor
        from datetime import datetime

        checker = MagicMock()
        checker.fetch_full_status.return_value = {'raw_response': {}}

        iteration = [0]

        def fake_now():
            iteration[0] += 1
            if iteration[0] <= 3:
                return datetime(2026, 3, 9, 10, 0, 0)
            return datetime(2026, 3, 9, 15, 0, 0)

        monitor = StatusMonitor(checker, duration_hours=4, poll_interval_seconds=42)

        with patch('amue.services.api.status_monitor.datetime') as mock_dt, \
             patch('amue.services.api.status_monitor.time') as mock_time:
            mock_dt.now.side_effect = fake_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            monitor.watch()

        mock_time.sleep.assert_called_with(42)
