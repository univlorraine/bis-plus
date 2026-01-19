# amue/utils/settings.py
"""
Configuration centralisée AMUE
Charge les paramètres depuis les variables Airflow avec validation
"""
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@dataclass
class AMUEConfig:
    """Configuration centralisée AMUE avec validation"""

    # API
    universite: str
    api_endpoint_admin: str
    api_endpoint_table: str
    api_max_retries: int = 3
    api_retry_delay_seconds: int = 30

    # Polling
    polling_interval_minutes: int = 10
    polling_max_wait_hours: int = 6
    polling_exponential_backoff: bool = False

    # Import
    import_batch_size: int = 5000
    import_max_memory_mb: int = 512

    # Historique
    max_history_days: int = 7

    # Email
    smtp_host: str = 'mailhog'
    smtp_port: int = 1025
    smtp_from: str = 'airflow@amue.local'
    report_recipients: List[str] = field(default_factory=lambda: ['admin@example.com'])

    # Environnement
    environment: str = 'production'

    def __post_init__(self):
        """Valide la configuration après création"""
        self._validate_universite()
        self._validate_retries()
        self._validate_polling()
        self._validate_batch_size()
        self._validate_email()
        logger.debug("[CONFIG] Configuration validée avec succès")

    def _validate_universite(self) -> None:
        """Valide le code université"""
        if not self.universite:
            raise ValueError("universite ne peut pas être vide")
        if not re.match(r'^[a-z0-9-]{2,50}$', self.universite.lower()):
            raise ValueError(
                f"universite invalide: '{self.universite}'. "
                "Format attendu: 2-50 caractères alphanumériques ou tirets."
            )

    def _validate_retries(self) -> None:
        """Valide les paramètres de retry"""
        if not 1 <= self.api_max_retries <= 10:
            raise ValueError(
                f"api_max_retries doit être entre 1 et 10 (reçu: {self.api_max_retries})"
            )
        if not 1 <= self.api_retry_delay_seconds <= 300:
            raise ValueError(
                f"api_retry_delay_seconds doit être entre 1 et 300 (reçu: {self.api_retry_delay_seconds})"
            )

    def _validate_polling(self) -> None:
        """Valide les paramètres de polling"""
        if not 1 <= self.polling_interval_minutes <= 120:
            raise ValueError(
                f"polling_interval_minutes doit être entre 1 et 120 (reçu: {self.polling_interval_minutes})"
            )
        if not 1 <= self.polling_max_wait_hours <= 24:
            raise ValueError(
                f"polling_max_wait_hours doit être entre 1 et 24 (reçu: {self.polling_max_wait_hours})"
            )

    def _validate_batch_size(self) -> None:
        """Valide la taille de batch"""
        if not 100 <= self.import_batch_size <= 50000:
            raise ValueError(
                f"import_batch_size doit être entre 100 et 50000 (reçu: {self.import_batch_size})"
            )

    def _validate_email(self) -> None:
        """Valide la configuration email"""
        if not 1 <= self.smtp_port <= 65535:
            raise ValueError(
                f"smtp_port doit être entre 1 et 65535 (reçu: {self.smtp_port})"
            )
        if not self.smtp_from or '@' not in self.smtp_from:
            raise ValueError(
                f"smtp_from invalide: '{self.smtp_from}'. Format attendu: email@domain.com"
            )

    @classmethod
    def from_airflow_variables(cls) -> 'AMUEConfig':
        """Charge depuis variables Airflow avec validation"""

        def get_var(key: str, default=None, required: bool = False):
            try:
                value = VarMgr.get(key, default=default)
                if value is None and required:
                    raise ValueError(f"Variable Airflow requise manquante: {key}")
                return value
            except Exception as e:
                if required:
                    raise ValueError(f"Variable Airflow requise manquante: {key}") from e
                return default

        recipients_str = get_var('amue_report_recipients', 'admin@example.com')
        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

        return cls(
            universite=get_var('universite', required=True),
            api_endpoint_admin=get_var('api_endpoint_admin', required=True),
            api_endpoint_table=get_var('api_endpoint_table', required=True),
            api_max_retries=int(get_var('amue_api_max_retries', 3)),
            api_retry_delay_seconds=int(get_var('amue_api_retry_delay_seconds', 30)),
            polling_interval_minutes=int(get_var('amue_polling_interval_minutes', 10)),
            polling_max_wait_hours=int(get_var('amue_max_wait_hours', 6)),
            polling_exponential_backoff=get_var('amue_polling_exponential_backoff', 'false').lower() == 'true',
            import_batch_size=int(get_var('amue_import_batch_size', 5000)),
            max_history_days=int(get_var('amue_max_history_days', 7)),
            smtp_host=get_var('smtp_host', 'mailhog'),
            smtp_port=int(get_var('smtp_port', 1025)),
            smtp_from=get_var('smtp_mail_from', 'airflow@amue.local'),
            report_recipients=recipients,
            environment=get_var('environment', 'production')
        )

    def is_production(self) -> bool:
        """Vérifie si l'environnement est production"""
        return self.environment.lower() == 'production'

    def is_development(self) -> bool:
        """Vérifie si l'environnement est development"""
        return self.environment.lower() == 'development'


# Instance globale (lazy loading)
_config: Optional[AMUEConfig] = None


def get_config() -> AMUEConfig:
    """Récupère la configuration globale (singleton)"""
    global _config
    if _config is None:
        _config = AMUEConfig.from_airflow_variables()
    return _config


def reload_config() -> AMUEConfig:
    """Force le rechargement de la configuration"""
    global _config
    _config = AMUEConfig.from_airflow_variables()
    return _config
