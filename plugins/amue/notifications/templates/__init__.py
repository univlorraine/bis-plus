# amue/notifications/templates/__init__.py
"""Templates d'emails pour les notifications AMUE"""
from amue.notifications.templates.base import BaseTemplate
from amue.notifications.templates.error import ErrorTemplate
from amue.notifications.templates.success import SuccessTemplate

__all__ = ['BaseTemplate', 'ErrorTemplate', 'SuccessTemplate']
