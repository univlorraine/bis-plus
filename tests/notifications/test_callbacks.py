# tests/notifications/test_callbacks.py
"""Tests unitaires pour les callbacks Airflow."""
from unittest.mock import MagicMock, patch


def make_context(with_exception=True, with_ti=True):
    """Construit un contexte Airflow minimal."""
    ctx = {}
    if with_exception:
        ctx['exception'] = Exception("Task failed")
    if with_ti:
        ti = MagicMock()
        ti.dag_id = 'test_dag'
        ti.xcom_pull.return_value = None
        ctx['task_instance'] = ti
    ctx['dag_run'] = MagicMock()
    ctx['execution_date'] = '2026-03-17T02:00:00'
    return ctx


class TestSendFailureNotification:
    """Tests pour send_failure_notification."""

    @patch('amue.notifications.notifier.NotificationService')
    def test_calls_notify_error(self, MockNS):
        """notify_error() est appelé avec le contexte Airflow."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        ctx = make_context()

        send_failure_notification(ctx)

        MockNS.return_value.notify_error.assert_called_once_with(ctx)

    @patch('amue.notifications.notifier.NotificationService')
    def test_skips_when_no_exception(self, MockNS):
        """Pas de notification si le contexte ne contient pas d'exception."""
        from amue.notifications.callbacks import send_failure_notification

        ctx = make_context(with_exception=False)
        send_failure_notification(ctx)

        MockNS.return_value.notify_error.assert_not_called()

    @patch('amue.notifications.notifier.NotificationService')
    def test_does_not_raise_on_notify_error_failure(self, MockNS):
        """Une exception dans notify_error() est swallowée."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.side_effect = Exception("SMTP down")
        ctx = make_context()

        # Ne doit pas lever
        send_failure_notification(ctx)

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_releases_bluegreen_lock_when_in_progress(self, MockNS, MockBGM):
        """Le verrou blue/green est libéré si un import est en cours."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        MockBGM.return_value.is_import_in_progress.return_value = True

        send_failure_notification(make_context())

        MockBGM.return_value.release_import_lock.assert_called_once_with(mark_completed=False)

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_no_lock_release_when_not_in_progress(self, MockNS, MockBGM):
        """Pas de libération de verrou si aucun import en cours."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        MockBGM.return_value.is_import_in_progress.return_value = False

        send_failure_notification(make_context())

        MockBGM.return_value.release_import_lock.assert_not_called()

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_does_not_raise_on_lock_release_failure(self, MockNS, MockBGM):
        """Exception lors de la libération du verrou est swallowée."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        MockBGM.return_value.is_import_in_progress.side_effect = Exception("BDD down")

        # Ne doit pas lever
        send_failure_notification(make_context())

    @patch('amue.notifications.report_generator.AMUEReportGenerator')
    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_generates_partial_report_when_xcom_results_available(self, MockNS, MockBGM, MockRG):
        """Génère un rapport partiel si des résultats d'import existent dans XCom."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        MockBGM.return_value.is_import_in_progress.return_value = False

        import_result = {'table_name': 'CSKS', 'rows_inserted': 1000}
        ctx = make_context()
        ctx['task_instance'].xcom_pull.return_value = [import_result]

        send_failure_notification(ctx)

        MockRG.return_value.generate_report.assert_called_once()
        call_args = MockRG.return_value.generate_report.call_args[0]
        assert import_result in call_args[0]

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_no_partial_report_when_no_ti(self, MockNS, MockBGM):
        """Pas de rapport partiel si task_instance absent du contexte."""
        from amue.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        MockBGM.return_value.is_import_in_progress.return_value = False

        ctx = make_context(with_ti=False)
        # Ne doit pas lever
        send_failure_notification(ctx)


class TestSendSuccessNotification:
    """Tests pour send_success_notification."""

    @patch('amue.notifications.notifier.NotificationService')
    def test_calls_notify_success(self, MockNS):
        """notify_success() est appelé."""
        from amue.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.return_value = True
        ctx = make_context(with_exception=False)

        send_success_notification(ctx)

        MockNS.return_value.notify_success.assert_called_once()

    @patch('amue.notifications.notifier.NotificationService')
    def test_dag_id_in_notification_data(self, MockNS):
        """Le dag_id est inclus dans les données de notification."""
        from amue.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.return_value = True
        ctx = make_context(with_exception=False)
        ctx['task_instance'].dag_id = 'amue_multi_table_import'

        send_success_notification(ctx)

        call_data = MockNS.return_value.notify_success.call_args[0][0]
        assert call_data['dag_id'] == 'amue_multi_table_import'

    @patch('amue.notifications.notifier.NotificationService')
    def test_does_not_raise_on_failure(self, MockNS):
        """Une exception dans notify_success() est swallowée."""
        from amue.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.side_effect = Exception("SMTP down")
        ctx = make_context(with_exception=False)

        # Ne doit pas lever
        send_success_notification(ctx)
