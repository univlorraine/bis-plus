# amue/notifications/notifiers/base.py
"""Classe de base pour les notifiers"""
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Optional

from amue.notifications.email_service import EmailService, Email
from amue.notifications.templates.base import BaseTemplate
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    """
    Classe de base pour les notifiers

    Fournit la logique commune pour:
    - Chargement des destinataires
    - Envoi des emails
    - Sauvegarde des rapports
    """

    def __init__(self, email_service: Optional[EmailService] = None):
        """
        Initialise le notifier

        Args:
            email_service: Service d'envoi d'emails (créé si non fourni)
        """
        self.email_service = email_service or EmailService()
        self.recipients = self._load_recipients()

    def _load_recipients(self) -> List[str]:
        """Charge la liste des destinataires depuis les variables Airflow"""
        recipients_var = VarMgr.get('amue_report_recipients', default='admin@example.com')
        recipients = [r.strip() for r in recipients_var.split(',') if r.strip()]
        logger.info(f"Destinataires chargés: {recipients}")
        return recipients

    @property
    @abstractmethod
    def template(self) -> BaseTemplate:
        """Template à utiliser pour le rendu"""
        pass

    @abstractmethod
    def build_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construit le contexte pour le template

        Args:
            data: Données brutes

        Returns:
            Contexte formaté pour le template
        """
        pass

    @abstractmethod
    def build_subject(self, context: Dict[str, Any]) -> str:
        """
        Construit le sujet de l'email

        Args:
            context: Contexte de la notification

        Returns:
            Sujet de l'email
        """
        pass

    def notify(self, data: Dict[str, Any]) -> bool:
        """
        Envoie une notification

        Args:
            data: Données de la notification

        Returns:
            True si envoi réussi
        """
        logger.info(f"Envoi notification {self.__class__.__name__}")

        # Construit le contexte
        context = self.build_context(data)

        # Construit le sujet et le contenu
        subject = self.build_subject(context)
        html_content = self.template.render(context)

        # Crée et envoie l'email
        email = Email(
            to=self.recipients,
            subject=subject,
            html_content=html_content
        )

        success = self.email_service.send(email)

        # Sauvegarde le rapport
        self._save_report(context, success)

        return success

    def _save_report(self, context: Dict[str, Any], success: bool) -> None:
        """
        Sauvegarde le rapport dans les variables Airflow

        Args:
            context: Contexte de la notification
            success: Si l'envoi a réussi
        """
        report = {
            'type': self.__class__.__name__,
            'timestamp': datetime.now().isoformat(),
            'email_sent': success,
            'recipients': self.recipients,
            **context
        }

        try:
            VarMgr.set('last_import_report', json.dumps(report), context['status'])
            logger.info("Rapport sauvegardé")
        except Exception as e:
            logger.warning(f"Échec sauvegarde rapport: {e}")
