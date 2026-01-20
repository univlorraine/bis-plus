"""
Tests unitaires pour les services de notification
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestEmailConfig:
    """Tests pour EmailConfig"""

    def test_email_config_defaults(self):
        """Valeurs par défaut de EmailConfig"""
        from amue.notifications.email_service import EmailConfig

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

    @patch('amue.notifications.email_service.VarMgr')
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

        from amue.notifications.email_service import EmailConfig

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
        """Création d'un email"""
        from amue.notifications.email_service import Email

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
        """Création d'un email avec texte"""
        from amue.notifications.email_service import Email

        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>',
            text_content='Hello'
        )

        assert email.text_content == 'Hello'


class TestEmailService:
    """Tests pour EmailService"""

    @patch('amue.notifications.email_service.VarMgr')
    def test_init_default_config(self, mock_varmgr):
        """Initialisation avec config par défaut"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.email_service import EmailService

        service = EmailService()

        assert service.config.host == 'mailhog'
        assert service.config.port == 1025

    def test_init_custom_config(self):
        """Initialisation avec config personnalisée"""
        from amue.notifications.email_service import EmailService, EmailConfig

        config = EmailConfig(
            host='custom.smtp.com',
            port=25,
            from_email='custom@example.com'
        )

        service = EmailService(config=config)

        assert service.config.host == 'custom.smtp.com'

    @patch('amue.notifications.email_service.smtplib.SMTP')
    @patch('amue.notifications.email_service.VarMgr')
    def test_send_success(self, mock_varmgr, mock_smtp):
        """Envoi réussi"""
        mock_varmgr.get.side_effect = lambda key, default=None: default
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        from amue.notifications.email_service import EmailService, Email

        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>'
        )

        result = service.send(email)

        assert result is True
        mock_server.sendmail.assert_called_once()

    @patch('amue.notifications.email_service.smtplib.SMTP')
    @patch('amue.notifications.email_service.VarMgr')
    def test_send_failure(self, mock_varmgr, mock_smtp):
        """Envoi échoué retourne False"""
        mock_varmgr.get.side_effect = lambda key, default=None: default
        mock_smtp.side_effect = Exception("SMTP Error")

        from amue.notifications.email_service import EmailService, Email

        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>'
        )

        result = service.send(email)

        assert result is False

    @patch('amue.notifications.email_service.VarMgr')
    def test_build_message(self, mock_varmgr):
        """Construction du message MIME"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.email_service import EmailService, Email

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
    """Tests pour NotificationService"""

    @patch('amue.notifications.notification_service.VarMgr')
    def test_init(self, mock_varmgr):
        """Initialisation du service"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': 'admin@example.com, user@example.com',
            'smtp_host': 'mailhog',
            'smtp_port': '1025',
            'smtp_mail_from': 'airflow@amue.local'
        }.get(key, default)

        from amue.notifications.notification_service import NotificationService

        service = NotificationService()

        assert len(service.recipients) == 2
        assert 'admin@example.com' in service.recipients
        assert service.smtp_host == 'mailhog'
        assert service.smtp_port == 1025

    @patch('amue.notifications.notification_service.VarMgr')
    def test_load_recipients(self, mock_varmgr):
        """Charge les destinataires"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': '  user1@test.com , user2@test.com  ',
            'smtp_host': 'mailhog',
            'smtp_port': '1025',
            'smtp_mail_from': 'airflow@amue.local'
        }.get(key, default)

        from amue.notifications.notification_service import NotificationService

        service = NotificationService()

        assert len(service.recipients) == 2
        assert 'user1@test.com' in service.recipients
        assert 'user2@test.com' in service.recipients

    @patch('amue.notifications.notification_service.smtplib.SMTP')
    @patch('amue.notifications.notification_service.VarMgr')
    def test_send_error_notification(self, mock_varmgr, mock_smtp):
        """Envoi notification d'erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_report_recipients': 'admin@example.com',
            'smtp_host': 'mailhog',
            'smtp_port': '1025',
            'smtp_mail_from': 'airflow@amue.local'
        }.get(key, default)
        mock_varmgr.set.return_value = True
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server

        from amue.notifications.notification_service import NotificationService, ErrorContext

        service = NotificationService()

        error_context = ErrorContext(
            execution_date='2024-01-15T10:30:00',
            dag_id='test_dag',
            task_id='test_task',
            error_message='Test error message',
            error_type='TestError'
        )

        service.send_error_notification(error_context)

        mock_server.sendmail.assert_called_once()

    @patch('amue.notifications.notification_service.VarMgr')
    def test_build_error_subject(self, mock_varmgr):
        """Construction du sujet d'erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notification_service import NotificationService, ErrorContext

        service = NotificationService()

        ctx = ErrorContext(
            execution_date='2024-01-15',
            dag_id='test_dag',
            task_id='test_task',
            error_message='Error',
            error_type='Error'
        )

        subject = service._build_error_subject(ctx)

        assert '[ERREUR]' in subject
        assert 'test_dag' in subject

    @patch('amue.notifications.notification_service.VarMgr')
    def test_build_error_html(self, mock_varmgr):
        """Construction du HTML d'erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notification_service import NotificationService, ErrorContext

        service = NotificationService()

        ctx = ErrorContext(
            execution_date='2024-01-15',
            dag_id='test_dag',
            task_id='test_task',
            error_message='Test error message',
            error_type='TestError',
            status='failed'
        )

        html = service._build_error_html(ctx)

        assert 'test_dag' in html
        assert 'test_task' in html
        assert 'Test error message' in html
        assert 'TestError' in html
        assert 'FAILED' in html

    @patch('amue.notifications.notification_service.VarMgr')
    def test_escape_html(self, mock_varmgr):
        """Échappement HTML"""
        mock_varmgr.get.side_effect = lambda key, default=None: default

        from amue.notifications.notification_service import NotificationService

        service = NotificationService()

        text = '<script>alert("XSS")</script>'
        escaped = service._escape_html(text)

        assert '<' not in escaped
        assert '>' not in escaped
        assert '&lt;' in escaped
        assert '&gt;' in escaped

    @patch('amue.notifications.notification_service.VarMgr')
    def test_save_error_report(self, mock_varmgr):
        """Sauvegarde du rapport d'erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: default
        mock_varmgr.set.return_value = True

        from amue.notifications.notification_service import NotificationService, ErrorContext

        service = NotificationService()

        ctx = ErrorContext(
            execution_date='2024-01-15',
            dag_id='test_dag',
            task_id='test_task',
            error_message='Error',
            error_type='Error'
        )

        service._save_error_report(ctx)

        mock_varmgr.set.assert_called_once()
        call_args = mock_varmgr.set.call_args[0]
        assert call_args[0] == 'last_import_report'


class TestErrorContext:
    """Tests pour ErrorContext dataclass"""

    def test_error_context_creation(self):
        """Création d'un contexte d'erreur"""
        from amue.notifications.notification_service import ErrorContext

        ctx = ErrorContext(
            execution_date='2024-01-15T10:30:00',
            dag_id='test_dag',
            task_id='test_task',
            error_message='An error occurred',
            error_type='ValueError'
        )

        assert ctx.execution_date == '2024-01-15T10:30:00'
        assert ctx.dag_id == 'test_dag'
        assert ctx.task_id == 'test_task'
        assert ctx.error_message == 'An error occurred'
        assert ctx.error_type == 'ValueError'
        assert ctx.status == 'failed'  # Default

    def test_error_context_custom_status(self):
        """Contexte d'erreur avec statut personnalisé"""
        from amue.notifications.notification_service import ErrorContext

        ctx = ErrorContext(
            execution_date='2024-01-15',
            dag_id='test_dag',
            task_id='test_task',
            error_message='Error',
            error_type='Error',
            status='retrying'
        )

        assert ctx.status == 'retrying'


class TestSendFailureNotification:
    """Tests pour la fonction callback send_failure_notification"""

    @patch('amue.notifications.notification_service.NotificationService')
    def test_send_failure_notification_with_exception(self, mock_service_class):
        """Callback avec exception"""
        mock_service = MagicMock()
        mock_service_class.return_value = mock_service

        from amue.notifications.notification_service import send_failure_notification

        task_instance = MagicMock()
        task_instance.dag_id = 'test_dag'
        task_instance.task_id = 'test_task'

        context = {
            'task_instance': task_instance,
            'exception': ValueError('Test error')
        }

        send_failure_notification(context)

        mock_service.send_error_notification.assert_called_once()

    @patch('amue.notifications.notification_service.NotificationService')
    def test_send_failure_notification_no_exception(self, mock_service_class):
        """Callback sans exception - notification ignorée"""
        mock_service = MagicMock()
        mock_service_class.return_value = mock_service

        from amue.notifications.notification_service import send_failure_notification

        context = {
            'task_instance': MagicMock(),
            'exception': None
        }

        send_failure_notification(context)

        mock_service.send_error_notification.assert_not_called()
