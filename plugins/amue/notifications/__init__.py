# amue/notifications/__init__.py
"""
Module de notifications AMUE.

================================================================================
ARCHITECTURE DU MODULE
================================================================================

Ce module gère toutes les notifications envoyées par le DAG :
    - Notifications d'ERREUR (échec de task ou de DAG)
    - Notifications de SUCCÈS (rapport de fin d'import)

STRUCTURE :
    notifications/
    ├── __init__.py          # Exports publics
    ├── email_service.py     # Service SMTP générique
    ├── notification_service.py  # Service legacy (rétro-compatibilité)
    ├── report_generator.py  # Génération des rapports d'import
    ├── notifiers/           # Notifiers spécialisés
    │   ├── base.py          # Classe abstraite BaseNotifier
    │   ├── error_notifier.py    # Notifications d'erreur
    │   └── success_notifier.py  # Notifications de succès
    └── templates/           # Templates HTML
        ├── base.py          # Template de base (header, footer)
        ├── error.py         # Template erreur (rouge)
        └── success.py       # Template succès (vert)

================================================================================
TYPES DE NOTIFICATIONS
================================================================================

ERREUR (ErrorNotifier / send_failure_notification) :
    - Déclenchée automatiquement via on_failure_callback
    - Email avec fond rouge, détails de l'erreur
    - Sauvegarde dans la variable 'last_import_report'

SUCCÈS (SuccessNotifier / AMUEReportGenerator) :
    - Déclenchée en fin de DAG via la task send_report
    - Email avec fond vert, statistiques d'import
    - Détail par table (lignes importées, type d'import)

================================================================================
CONFIGURATION SMTP
================================================================================

Variables Airflow :
    - smtp_host              : Serveur SMTP (défaut: mailhog)
    - smtp_port              : Port SMTP (défaut: 1025)
    - smtp_mail_from         : Adresse expéditeur
    - smtp_use_tls           : Activer TLS (défaut: false)
    - smtp_username          : Login SMTP (optionnel)
    - smtp_password          : Mot de passe SMTP (optionnel)
    - amue_report_recipients : Destinataires (CSV)

================================================================================
"""

# Service email
from amue.notifications.email_service import EmailService, EmailConfig, Email
# Notifiers
from amue.notifications.notifiers import BaseNotifier, ErrorNotifier, SuccessNotifier
from amue.notifications.notifiers.error_notifier import send_failure_notification
# Templates
from amue.notifications.templates import BaseTemplate, ErrorTemplate, SuccessTemplate

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
