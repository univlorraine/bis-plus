# amue/utils/settings.py
"""
Configuration centralisée AMUE
Charge les paramètres depuis les variables Airflow avec validation
"""
from dataclasses import dataclass, field
from typing import List, Optional
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr


@dataclass
class AMUEConfig:
    """Configuration centralisée AMUE"""

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
