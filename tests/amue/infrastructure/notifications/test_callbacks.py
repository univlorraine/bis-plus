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
    """Tests pour send_failure_notification (callback niveau task — email uniquement)."""

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_calls_notify_error(self, MockNS):
        """notify_error() est appelé avec le contexte Airflow."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        ctx = make_context()

        send_failure_notification(ctx)

        MockNS.return_value.notify_error.assert_called_once_with(ctx)

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_sends_notification_at_dag_level_without_exception(self, MockNS):
        """notify_error() est appelé même sans exception (callback niveau DAG)."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        ctx = make_context(with_exception=False)
        send_failure_notification(ctx)

        MockNS.return_value.notify_error.assert_called_once()

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_does_not_raise_on_notify_error_failure(self, MockNS):
        """Une exception dans notify_error() est swallowée."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.side_effect = Exception("SMTP down")
        ctx = make_context()

        # Ne doit pas lever
        send_failure_notification(ctx)

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_dag_level_extracts_failed_tasks(self, MockNS):
        """Au niveau DAG (sans exception), failed_tasks est extrait du dag_run."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True

        failed_ti = MagicMock()
        failed_ti.task_id = 'import_data'
        failed_ti.map_index = 2
        failed_ti.duration = 12.5

        ctx = make_context(with_exception=False)
        ctx['dag_run'].get_task_instances.return_value = [failed_ti]

        send_failure_notification(ctx)

        assert 'failed_tasks' in ctx
        assert ctx['failed_tasks'][0]['task_id'] == 'import_data'
        assert ctx['failed_tasks'][0]['map_index'] == 2
        assert ctx['failed_tasks'][0]['duration'] == 12.5
        assert 'import_data[2]' in ctx['error_message']

    @patch('amue.infrastructure.notifications.report_generator.AMUEReportGenerator')
    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_generates_partial_report_when_xcom_results_available(self, MockNS, MockRG):
        """Génère un rapport partiel via dag_run (API Airflow 3 : map_index entier)."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True

        import_result = {'table_name': 'CSKS', 'rows_inserted': 1000}

        # Simule une TI import_data réussie retournée par dag_run
        success_ti = MagicMock()
        success_ti.task_id = 'import_data'
        success_ti.map_index = 0

        ctx = make_context()
        ctx['dag_run'].get_task_instances.return_value = [success_ti]
        ctx['task_instance'].xcom_pull.return_value = import_result

        send_failure_notification(ctx)

        MockRG.return_value.generate_report.assert_called_once()
        call_args = MockRG.return_value.generate_report.call_args[0]
        assert import_result in call_args[0]

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_no_partial_report_when_no_ti(self, MockNS):
        """Pas de rapport partiel si task_instance absent du contexte."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True

        ctx = make_context(with_ti=False)
        # Ne doit pas lever
        send_failure_notification(ctx)

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_does_not_call_bluegreen_manager(self, MockNS, MockBGM):
        """send_failure_notification ne touche pas au blue/green (rollback délégué à dag_failure_rollback)."""
        from amue.infrastructure.notifications.callbacks import send_failure_notification

        MockNS.return_value.notify_error.return_value = True
        ctx = make_context()

        send_failure_notification(ctx)

        MockBGM.assert_not_called()


class TestDagFailureRollback:
    """Tests pour dag_failure_rollback (callback niveau DAG — rollback blue/green uniquement)."""

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_releases_bluegreen_lock_when_in_progress(self, MockBGM):
        """Le verrou blue/green est libéré si un import est en cours."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.return_value = True
        MockBGM.return_value.get_target_schema.return_value = 'splus_green'

        dag_failure_rollback(make_context())

        MockBGM.return_value.release_import_lock.assert_called_once_with(mark_completed=False)

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_renames_target_schema_to_offline_on_failure(self, MockBGM):
        """Le schéma cible est renommé en _offline après libération du verrou."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.return_value = True
        MockBGM.return_value.get_target_schema.return_value = 'splus_green'

        dag_failure_rollback(make_context())

        MockBGM.return_value.rename_schema_to_offline.assert_called_once_with('splus_green')

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_rename_called_even_when_not_in_progress(self, MockBGM):
        """rename_schema_to_offline est toujours appelé (idempotent), même sans verrou actif."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.return_value = False
        MockBGM.return_value.get_target_schema.return_value = 'splus_green'

        dag_failure_rollback(make_context())

        MockBGM.return_value.rename_schema_to_offline.assert_called_once_with('splus_green')
        MockBGM.return_value.release_import_lock.assert_not_called()

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_no_lock_release_when_not_in_progress(self, MockBGM):
        """Pas de libération de verrou si aucun import en cours."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.return_value = False

        dag_failure_rollback(make_context())

        MockBGM.return_value.release_import_lock.assert_not_called()

    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_does_not_raise_on_lock_release_failure(self, MockBGM):
        """Exception lors de la libération du verrou est swallowée."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.side_effect = Exception("BDD down")

        # Ne doit pas lever
        dag_failure_rollback(make_context())

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    @patch('common.application.bluegreen.bluegreen_manager.BlueGreenManager')
    def test_does_not_send_email(self, MockBGM, MockNS):
        """dag_failure_rollback n'envoie pas d'email (c'est le rôle de send_failure_notification)."""
        from amue.infrastructure.notifications.callbacks import dag_failure_rollback

        MockBGM.return_value.is_import_in_progress.return_value = False

        dag_failure_rollback(make_context())

        MockNS.assert_not_called()


class TestSendSuccessNotification:
    """Tests pour send_success_notification."""

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_calls_notify_success(self, MockNS):
        """notify_success() est appelé."""
        from amue.infrastructure.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.return_value = True
        ctx = make_context(with_exception=False)

        send_success_notification(ctx)

        MockNS.return_value.notify_success.assert_called_once()

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_dag_id_in_notification_data(self, MockNS):
        """Le dag_id est inclus dans les données de notification."""
        from amue.infrastructure.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.return_value = True
        ctx = make_context(with_exception=False)
        ctx['task_instance'].dag_id = 'amue_multi_table_import'

        send_success_notification(ctx)

        call_data = MockNS.return_value.notify_success.call_args[0][0]
        assert call_data['dag_id'] == 'amue_multi_table_import'

    @patch('amue.infrastructure.notifications.notifier.NotificationService')
    def test_does_not_raise_on_failure(self, MockNS):
        """Une exception dans notify_success() est swallowée."""
        from amue.infrastructure.notifications.callbacks import send_success_notification

        MockNS.return_value.notify_success.side_effect = Exception("SMTP down")
        ctx = make_context(with_exception=False)

        # Ne doit pas lever
        send_success_notification(ctx)
