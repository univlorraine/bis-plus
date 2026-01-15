# amue/notifications/__init__.py
"""
Module de notifications AMUE

Architecture:
- email_service: Service SMTP générique
- templates/: Templates HTML pour les emails
- notifiers/: Notifiers spécialisés (erreur, succès)
"""

# Service email
from amue.notifications.email_service import EmailService, EmailConfig, Email

# Templates
from amue.notifications.templates import BaseTemplate, ErrorTemplate, SuccessTemplate

# Notifiers
from amue.notifications.notifiers import BaseNotifier, ErrorNotifier, SuccessNotifier
from amue.notifications.notifiers.error_notifier import send_failure_notification

__all__ = [
    # Service
    'EmailService',
    'EmailConfig',
    'Email',
    # Templates
    'BaseTemplate',
    'ErrorTemplate',
    'SuccessTemplate',
    # Notifiers
    'BaseNotifier',
    'ErrorNotifier',
    'SuccessNotifier',
    # Callbacks
    'send_failure_notification',
]
