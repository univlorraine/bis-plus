"""
Tests unitaires pour les services de notification
"""
import pytest
from unittest.mock import MagicMock, patch


class TestEmailConfig:
    """Tests pour EmailConfig"""

    def test_email_config_defaults(self):
        """Valeurs par defaut de EmailConfig"""
        from common.notifications.email_service import EmailConfig

        config = EmailConfig(
            host='mailhog',
            port=1025,
            from_email='test@example.com'
        )

        assert config.host == 'mailhog'
        assert config.port == 1025
        assert config.from_email == 'test@example.com'
        assert config.use_tls is False
        assert config.username is None
        assert config.password is None
        assert config.timeout == 30

    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_email_config_from_airflow(self, mock_varmgr):
        """Charge la config depuis Airflow"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'smtp_host': 'smtp.example.com',
            'smtp_port': '587',
            'smtp_mail_from': 'airflow@example.com',
            'smtp_use_tls': 'true',
            'smtp_username': 'user',
            'smtp_password': 'pass',
            'smtp_timeout': '60'
        }.get(key, default)

        from common.notifications.email_service import EmailConfig

        config = EmailConfig.from_airflow_variables()

        assert config.host == 'smtp.example.com'
        assert config.port == 587
        assert config.from_email == 'airflow@example.com'
        assert config.use_tls is True
        assert config.username == 'user'
        assert config.password == 'pass'
        assert config.timeout == 60


class TestEmail:
    """Tests pour Email dataclass"""

    def test_email_creation(self):
        """Creation d'un email"""
        from common.notifications.email_service import Email

        email = Email(
            to=['user@example.com', 'admin@example.com'],
            subject='Test Subject',
            html_content='<h1>Hello</h1>'
        )

        assert email.to == ['user@example.com', 'admin@example.com']
        assert email.subject == 'Test Subject'
        assert email.html_content == '<h1>Hello</h1>'
        assert email.text_content is None

    def test_email_with_text(self):
        """Creation d'un email avec texte"""
        from common.notifications.email_service import Email

        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>',
            text_content='Hello'
        )

        assert email.text_content == 'Hello'


class TestEmailService:
    """Tests pour EmailService"""

    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_init_default_config(self, mock_varmgr):
        """Initialisation avec config par defaut"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from common.notifications.email_service import EmailService

        service = EmailService()

        assert service.config.host == 'mailhog'
        assert service.config.port == 1025

    def test_init_custom_config(self):
        """Initialisation avec config personnalisee"""
        from common.notifications.email_service import EmailService, EmailConfig

        config = EmailConfig(
            host='custom.smtp.com',
            port=25,
            from_email='custom@example.com'
        )

        service = EmailService(config=config)

        assert service.config.host == 'custom.smtp.com'

    @patch('common.notifications.email_service.smtplib.SMTP')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_send_success(self, mock_varmgr, mock_smtp):
        """Envoi reussi"""
        mock_varmgr.get.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        from common.notifications.email_service import EmailService, Email

        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>'
        )

        result = service.send(email)

        assert result is True
        mock_server.sendmail.assert_called_once()

    @patch('common.notifications.email_service.smtplib.SMTP')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_send_failure(self, mock_varmgr, mock_smtp):
        """Envoi echoue retourne False"""
        mock_varmgr.get.side_effect = lambda key, default=None: default
        mock_smtp.side_effect = Exception("SMTP Error")

        from common.notifications.email_service import EmailService, Email

        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>'
        )

        result = service.send(email)

        assert result is False

    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_build_message(self, mock_varmgr):
        """Construction du message MIME"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from common.notifications.email_service import EmailService, Email

        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test Subject',
            html_content='<h1>Hello</h1>',
            text_content='Hello plain'
        )

        msg = service._build_message(email)

        assert msg['Subject'] == 'Test Subject'
        assert msg['To'] == 'user@example.com'
        # Le message contient 2 parties (text + html)
        assert len(msg.get_payload()) == 2


class TestNotificationService:
    """Tests pour NotificationService (nouvelle architecture)"""

    @patch('amue.notifications.notifier.VarMgr')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_init(self, mock_email_varmgr, mock_notifier_varmgr):
        """Initialisation du service"""
        mock_notifier_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': 'admin@example.com, user@example.com',
        }.get(key, default)
        mock_email_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        assert len(service.recipients) == 2
        assert 'admin@example.com' in service.recipients

    @patch('amue.notifications.notifier.VarMgr')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_load_recipients(self, mock_email_varmgr, mock_notifier_varmgr):
        """Charge les destinataires"""
        mock_notifier_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': '  user1@test.com , user2@test.com  ',
        }.get(key, default)
        mock_email_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        assert len(service.recipients) == 2
        assert 'user1@test.com' in service.recipients
        assert 'user2@test.com' in service.recipients

    @patch('amue.notifications.notifier.VarMgr')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    @patch('common.notifications.email_service.smtplib.SMTP')
    def test_notify_error(self, mock_smtp, mock_email_varmgr, mock_notifier_varmgr):
        """Envoi notification d'erreur"""
        mock_notifier_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': 'admin@example.com',
        }.get(key, default)
        mock_notifier_varmgr.set.return_value = True
        mock_email_varmgr.get.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        error_data = {
            'dag_id': 'test_dag',
            'task_id': 'test_task',
            'error_message': 'Test error message',
            'error_type': 'TestError'
        }

        result = service.notify_error(error_data)

        assert result is True
        mock_server.sendmail.assert_called_once()

    @patch('amue.notifications.notifier.VarMgr')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_build_error_context(self, mock_email_varmgr, mock_notifier_varmgr):
        """Construction du contexte d'erreur"""
        mock_notifier_varmgr.get.side_effect = lambda key, default=None: default
        mock_email_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        data = {
            'dag_id': 'test_dag',
            'task_id': 'test_task',
            'error_message': 'Test error',
            'error_type': 'TestError'
        }

        context = service._build_error_context(data)

        assert context['dag_id'] == 'test_dag'
        assert context['task_id'] == 'test_task'
        assert context['error_message'] == 'Test error'
        assert context['error_type'] == 'TestError'
        assert context['status'] == 'failed'

    @patch('amue.notifications.notifier.VarMgr')
    @patch('amue.utils.config.airflow_helpers.AirflowVariableManager')
    def test_build_error_subject(self, mock_email_varmgr, mock_notifier_varmgr):
        """Construction du sujet d'erreur"""
        mock_notifier_varmgr.get.side_effect = lambda key, default=None: default
        mock_email_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notifier import NotificationService

        service = NotificationService()

        context = {
            'dag_id': 'test_dag',
            'task_id': 'test_task'
        }

        subject = service._build_error_subject(context)

        assert '[ERREUR]' in subject
        assert 'test_dag' in subject


class TestNotificationTemplates:
    """Tests pour NotificationTemplates"""

    def test_escape_html(self):
        """Echappement HTML"""
        from amue.notifications.templates import NotificationTemplates

        text = '<script>alert("XSS")</script>'
        escaped = NotificationTemplates.escape_html(text)

        assert '<' not in escaped
        assert '>' not in escaped
        assert '&lt;' in escaped
        assert '&gt;' in escaped

    def test_render_error(self):
        """Rendu du template d'erreur"""
        from amue.notifications.templates import NotificationTemplates

        context = {
            'title': 'Erreur Test',
            'subtitle': '2024-01-15',
            'dag_id': 'test_dag',
            'task_id': 'test_task',
            'error_message': 'Test error message',
            'error_type': 'TestError',
            'status': 'failed'
        }

        html = NotificationTemplates.render_error(context)

        assert 'test_dag' in html
        assert 'test_task' in html
        assert 'Test error message' in html
        assert 'TestError' in html
        assert 'failed' in html

    def test_render_success(self):
        """Rendu du template de succes"""
        from amue.notifications.templates import NotificationTemplates

        context = {
            'title': 'Import Reussi',
            'subtitle': '2024-01-15',
            'dag_id': 'test_dag',
            'execution_date': '2024-01-15',
            'duration': '5m 30s',
            'tables_imported': [
                {'table_name': 'TABLE1', 'rows_fetched': 100, 'rows_inserted': 100, 'status': 'success'}
            ],
            'total_rows': 100,
            'total_fetched': 100
        }

        html = NotificationTemplates.render_success(context)

        assert 'test_dag' in html
        assert 'TABLE1' in html
        assert '100' in html


class TestSendFailureNotification:
    """Tests pour la fonction callback send_failure_notification"""

    @patch('amue.notifications.notifier.NotificationService')
    def test_send_failure_notification_with_exception(self, mock_service_class):
        """Callback avec exception"""
        mock_service = MagicMock()
        mock_service.notify_error.return_value = True
        mock_service_class.return_value = mock_service

        from amue.notifications.callbacks import send_failure_notification

        task_instance = MagicMock()
        task_instance.dag_id = 'test_dag'
        task_instance.task_id = 'test_task'

        context = {
            'task_instance': task_instance,
            'exception': ValueError('Test error')
        }

        send_failure_notification(context)

        mock_service.notify_error.assert_called_once()

    @patch('amue.notifications.notifier.NotificationService')
    def test_send_failure_notification_no_exception(self, mock_service_class):
        """Callback sans exception - notification ignoree"""
        mock_service = MagicMock()
        mock_service_class.return_value = mock_service

        from amue.notifications.callbacks import send_failure_notification

        context = {
            'task_instance': MagicMock(),
            'exception': None
        }

        send_failure_notification(context)

        mock_service.notify_error.assert_not_called()

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_send_failure_releases_bluegreen_lock(self, mock_service_class, mock_bg_class):
        """Callback libere le verrou blue/green si actif et import en cours"""
        mock_service = MagicMock()
        mock_service.notify_error.return_value = True
        mock_service_class.return_value = mock_service

        mock_manager = MagicMock()
        mock_manager.is_import_in_progress.return_value = True
        mock_bg_class.return_value = mock_manager

        from amue.notifications.callbacks import send_failure_notification

        context = {
            'task_instance': MagicMock(),
            'exception': ValueError('Test error')
        }

        send_failure_notification(context)

        mock_manager.release_import_lock.assert_called_once_with(mark_completed=False)

    @patch('amue.services.bluegreen.bluegreen_manager.BlueGreenManager')
    @patch('amue.notifications.notifier.NotificationService')
    def test_send_failure_no_release_when_no_import_in_progress(self, mock_service_class, mock_bg_class):
        """Callback ne libere pas le verrou si aucun import en cours"""
        mock_service = MagicMock()
        mock_service.notify_error.return_value = True
        mock_service_class.return_value = mock_service

        mock_manager = MagicMock()
        mock_manager.is_enabled.return_value = True
        mock_manager.is_import_in_progress.return_value = False
        mock_bg_class.return_value = mock_manager

        from amue.notifications.callbacks import send_failure_notification

        context = {
            'task_instance': MagicMock(),
            'exception': ValueError('Test error')
        }

        send_failure_notification(context)

        mock_manager.release_import_lock.assert_not_called()
