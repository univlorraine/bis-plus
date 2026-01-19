# amue/notifications/email_service.py
"""
Service d'envoi d'emails via SMTP
Gère la connexion et l'envoi de manière générique
"""
import logging
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@dataclass
class EmailConfig:
    """Configuration SMTP"""
    host: str
    port: int
    from_email: str
    use_tls: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    timeout: int = 30  # Timeout en secondes

    @classmethod
    def from_airflow_variables(cls) -> 'EmailConfig':
        """Charge la configuration depuis les variables Airflow"""
        return cls(
            host=VarMgr.get('smtp_host', default='mailhog'),
            port=int(VarMgr.get('smtp_port', default='1025')),
            from_email=VarMgr.get('smtp_mail_from', default='airflow@amue.local'),
            use_tls=VarMgr.get('smtp_use_tls', default='false').lower() == 'true',
            username=VarMgr.get('smtp_username', default=None),
            password=VarMgr.get('smtp_password', default=None),
            timeout=int(VarMgr.get('smtp_timeout', default='30'))
        )


@dataclass
class Email:
    """Représente un email à envoyer"""
    to: List[str]
    subject: str
    html_content: str
    text_content: Optional[str] = None


class EmailService:
    """
    Service d'envoi d'emails via SMTP

    Usage:
        service = EmailService()
        email = Email(
            to=['user@example.com'],
            subject='Test',
            html_content='<h1>Hello</h1>'
        )
        service.send(email)
    """

    def __init__(self, config: Optional[EmailConfig] = None):
        """
        Initialise le service email

        Args:
            config: Configuration SMTP (charge depuis Airflow si non fournie)
        """
        self.config = config or EmailConfig.from_airflow_variables()
        logger.info(f"EmailService initialisé: {self.config.host}:{self.config.port}")

    def send(self, email: Email) -> bool:
        """
        Envoie un email

        Args:
            email: Email à envoyer

        Returns:
            True si envoi réussi, False sinon
        """
        try:
            msg = self._build_message(email)
            self._send_smtp(msg, email.to)
            logger.info(f"Email envoyé à {', '.join(email.to)}")
            return True
        except Exception as e:
            logger.error(f"Échec envoi email: {e}")
            return False

    def _build_message(self, email: Email) -> MIMEMultipart:
        """Construit le message MIME"""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = email.subject
        msg['From'] = self.config.from_email
        msg['To'] = ', '.join(email.to)

        # Ajoute le texte brut si fourni
        if email.text_content:
            msg.attach(MIMEText(email.text_content, 'plain', 'utf-8'))

        # Ajoute le contenu HTML
        msg.attach(MIMEText(email.html_content, 'html', 'utf-8'))

        return msg

    def _send_smtp(self, msg: MIMEMultipart, recipients: List[str]) -> None:
        """Envoie via SMTP avec timeout"""
        with smtplib.SMTP(
            self.config.host,
            self.config.port,
            timeout=self.config.timeout
        ) as server:
            if self.config.use_tls:
                server.starttls()

            if self.config.username and self.config.password:
                server.login(self.config.username, self.config.password)

            server.sendmail(self.config.from_email, recipients, msg.as_string())
