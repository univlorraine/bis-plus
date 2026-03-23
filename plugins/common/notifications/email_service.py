# common/notifications/email_service.py
"""
Service d'envoi d'emails via SMTP.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module fournit un service SMTP générique réutilisable par tous les
composants du projet. Il est indépendant de la logique métier AMUE/ECC.

COMPOSANTS :
    - EmailConfig : Configuration SMTP (host, port, auth, TLS)
    - Email : Représentation d'un email (destinataires, sujet, contenu)
    - EmailService : Service d'envoi avec gestion des erreurs

================================================================================
CONFIGURATION
================================================================================

La configuration peut être :
    1. Fournie explicitement via EmailConfig
    2. Chargée automatiquement depuis les variables Airflow

Variables Airflow utilisées :
    - smtp_host       : Serveur SMTP (défaut: mailhog)
    - smtp_port       : Port SMTP (défaut: 1025)
    - smtp_mail_from  : Adresse expéditeur
    - smtp_use_tls    : Activer STARTTLS (défaut: false)
    - smtp_username   : Login SMTP (optionnel)
    - smtp_password   : Mot de passe SMTP (optionnel)
    - smtp_timeout    : Timeout connexion en secondes (défaut: 30)

================================================================================
USAGE
================================================================================

    from common.notifications.email_service import EmailService, Email

    # Configuration automatique depuis Airflow
    service = EmailService()

    # Création de l'email
    email = Email(
        to=['admin@example.com', 'backup@example.com'],
        subject='[AMUE] Rapport d\'import',
        html_content='<h1>Import réussi</h1><p>15 tables importées</p>'
    )

    # Envoi
    success = service.send(email)
    if not success:
        print("Erreur d'envoi")

================================================================================
ENVIRONNEMENT DE DÉVELOPPEMENT
================================================================================

En développement, le projet utilise MailHog (mailhog:1025) qui :
    - Capture tous les emails sans les envoyer
    - Fournit une UI web sur http://localhost:8025
    - Ne nécessite pas d'authentification

================================================================================
"""
import logging
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

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
        # Import local pour éviter les imports circulaires (common → amue → common)
        from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
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
