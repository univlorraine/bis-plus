# amue/notifications/__init__.py
"""
Module de notifications AMUE.

================================================================================
ARCHITECTURE DU MODULE
================================================================================

Ce module gere toutes les notifications envoyees par le DAG :
    - Notifications d'ERREUR (echec de task ou de DAG)
    - Notifications de SUCCES (rapport de fin d'import)

STRUCTURE :
    notifications/
    |-- __init__.py          # Exports publics
    |-- email_service.py     # Service SMTP generique
    |-- templates.py         # Templates HTML (succes + erreur)
    |-- notifier.py          # Service de notification unifie
    |-- callbacks.py         # Callbacks Airflow
    |-- report_generator.py  # Generation des rapports d'import

================================================================================
TYPES DE NOTIFICATIONS
================================================================================

ERREUR (NotificationService.notify_error / send_failure_notification) :
    - Declenchee automatiquement via on_failure_callback
    - Email avec fond rouge, details de l'erreur
    - Email avec fond rouge, details de l'erreur

SUCCES (NotificationService.notify_success / AMUEReportGenerator) :
    - Declenchee en fin de DAG via la task send_report
    - Email avec fond vert, statistiques d'import
    - Detail par table (lignes importees, type d'import)

================================================================================
CONFIGURATION SMTP
================================================================================

Variables Airflow :
    - smtp_host              : Serveur SMTP (defaut: mailhog)
    - smtp_port              : Port SMTP (defaut: 1025)
    - smtp_mail_from         : Adresse expediteur
    - smtp_use_tls           : Activer TLS (defaut: false)
    - smtp_username          : Login SMTP (optionnel)
    - smtp_password          : Mot de passe SMTP (optionnel)
    - amue_report_recipients : Destinataires (CSV)

================================================================================
"""

# Service email
from amue.notifications.email_service import EmailService, EmailConfig, Email

# Templates unifies
from amue.notifications.templates import NotificationTemplates

# Service de notification unifie
from amue.notifications.notifier import NotificationService

# Callbacks Airflow
from amue.notifications.callbacks import send_failure_notification, send_success_notification

__all__ = [
    # Service email
    'EmailService',
    'EmailConfig',
    'Email',
    # Templates
    'NotificationTemplates',
    # Service de notification
    'NotificationService',
    # Callbacks
    'send_failure_notification',
    'send_success_notification',
]
