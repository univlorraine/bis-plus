# ecc/notifications/__init__.py
"""Notifications ECC — templates, service et callbacks."""
from ecc.notifications.ecc_templates import ECCNotificationTemplates
from ecc.notifications.ecc_notifier import ECCNotificationService
from ecc.notifications.ecc_callbacks import send_ecc_failure_notification

__all__ = [
    'ECCNotificationTemplates',
    'ECCNotificationService',
    'send_ecc_failure_notification',
]
