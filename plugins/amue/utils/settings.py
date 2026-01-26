"""
Configuration centralisée AMUE
Charge les paramètres depuis les variables Airflow avec validation

================================================================================
VARIABLES AIRFLOW UTILISÉES
================================================================================

OBLIGATOIRES :
--------------
  universite                    Code université (ex: "ul", "unistra")
                                Format: 2-50 caractères alphanumériques ou tirets

  api_endpoint_admin            Endpoint API admin avec placeholder $univ
                                Ex: "/sifacweb/admin/$univ"

  api_endpoint_table            Endpoint API données avec placeholders $univ et $table
                                Ex: "/sifacweb/data/$univ/$table"

OPTIONNELLES (avec valeurs par défaut) :
----------------------------------------
  environment                   Environnement d'exécution
                                Valeurs: "dev" | "production"
                                Défaut: "production"
                                Impact: En production, création de tables interdite

  amue_tables_to_import         Liste JSON des tables à importer
                                Format: [{"name": "CSKS", "primary_key": "", ...}, ...]

  amue_api_max_retries          Nombre max de tentatives API en cas d'erreur
                                Valeurs: 1-10
                                Défaut: 3

  amue_api_retry_delay_seconds  Délai entre les tentatives (secondes)
                                Valeurs: 1-300
                                Défaut: 30

  amue_polling_interval_minutes Intervalle entre les vérifications de disponibilité API
                                Valeurs: 1-120
                                Défaut: 10

  amue_max_wait_hours           Durée max d'attente de l'API (heures)
                                Valeurs: 1-24
                                Défaut: 6

  amue_polling_exponential_backoff  Active le backoff exponentiel pour le polling
                                    Valeurs: "true" | "false"
                                    Défaut: "false"

  amue_import_batch_size        Nombre de lignes par batch d'import
                                Valeurs: 100-50000
                                Défaut: 5000

  amue_max_history_days         Nombre de jours d'historique à vérifier
                                Valeurs: 1-30
                                Défaut: 7

  smtp_host                     Serveur SMTP pour les notifications
                                Défaut: "mailhog"

  smtp_port                     Port SMTP
                                Valeurs: 1-65535
                                Défaut: 1025

  smtp_mail_from                Adresse expéditeur des emails
                                Format: email valide
                                Défaut: "airflow@amue.local"

  amue_report_recipients        Destinataires des rapports (séparés par virgule)
                                Ex: "admin@example.com,backup@example.com"
                                Défaut: "admin@example.com"

VARIABLES INTERNES (gérées automatiquement) :
---------------------------------------------
  amue_last_successful_run      Date ISO du dernier import réussi
                                Mis à jour automatiquement après chaque succès

  _current_run_polling          Données de polling de l'exécution courante
                                Variable temporaire, nettoyée après chaque run

================================================================================
"""
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@dataclass
class AMUEConfig:
    """
    Configuration centralisée AMUE avec validation

    Toutes les valeurs sont chargées depuis les variables Airflow.
    Voir la docstring du module pour la liste complète des variables.
    """

    # --- API ---
    universite: str                     # Variable: universite (obligatoire)
    api_endpoint_admin: str             # Variable: api_endpoint_admin (obligatoire)
    api_endpoint_table: str             # Variable: api_endpoint_table (obligatoire)
    api_max_retries: int = 3            # Variable: amue_api_max_retries
    api_retry_delay_seconds: int = 30   # Variable: amue_api_retry_delay_seconds

    # --- Polling ---
    polling_interval_minutes: int = 10  # Variable: amue_polling_interval_minutes
    polling_max_wait_hours: int = 6     # Variable: amue_max_wait_hours
    polling_exponential_backoff: bool = False  # Variable: amue_polling_exponential_backoff

    # --- Import ---
    import_batch_size: int = 5000       # Variable: amue_import_batch_size
    import_max_memory_mb: int = 512     # Non configurable via variable

    # --- Historique ---
    max_history_days: int = 7           # Variable: amue_max_history_days

    # --- Email ---
    smtp_host: str = 'mailhog'          # Variable: smtp_host
    smtp_port: int = 1025               # Variable: smtp_port
    smtp_from: str = 'airflow@amue.local'  # Variable: smtp_mail_from
    report_recipients: List[str] = field(default_factory=lambda: ['admin@example.com'])
                                        # Variable: amue_report_recipients

    # --- Environnement ---
    environment: str = 'production'     # Variable: environment ("dev" ou "production")

    def __post_init__(self):
        """Valide la configuration après création"""
        self._validate_universite()
        self._validate_retries()
        self._validate_polling()
        self._validate_batch_size()
        self._validate_email()
        logger.debug("[CONFIG] Configuration validée avec succès")

    def _validate_universite(self) -> None:
        """Valide le code université (2-50 chars alphanumériques)"""
        if not self.universite:
            raise ValueError("universite ne peut pas être vide")
        if not re.match(r'^[a-z0-9-]{2,50}$', self.universite.lower()):
            raise ValueError(
                f"universite invalide: '{self.universite}'. "
                "Format attendu: 2-50 caractères alphanumériques ou tirets."
            )

    def _validate_retries(self) -> None:
        """Valide: amue_api_max_retries (1-10), amue_api_retry_delay_seconds (1-300)"""
        if not 1 <= self.api_max_retries <= 10:
            raise ValueError(
                f"api_max_retries doit être entre 1 et 10 (reçu: {self.api_max_retries})"
            )
        if not 1 <= self.api_retry_delay_seconds <= 300:
            raise ValueError(
                f"api_retry_delay_seconds doit être entre 1 et 300 (reçu: {self.api_retry_delay_seconds})"
            )

    def _validate_polling(self) -> None:
        """Valide: amue_polling_interval_minutes (1-120), amue_max_wait_hours (1-24)"""
        if not 1 <= self.polling_interval_minutes <= 120:
            raise ValueError(
                f"polling_interval_minutes doit être entre 1 et 120 (reçu: {self.polling_interval_minutes})"
            )
        if not 1 <= self.polling_max_wait_hours <= 24:
            raise ValueError(
                f"polling_max_wait_hours doit être entre 1 et 24 (reçu: {self.polling_max_wait_hours})"
            )

    def _validate_batch_size(self) -> None:
        """Valide: amue_import_batch_size (100-50000)"""
        if not 100 <= self.import_batch_size <= 50000:
            raise ValueError(
                f"import_batch_size doit être entre 100 et 50000 (reçu: {self.import_batch_size})"
            )

    def _validate_email(self) -> None:
        """Valide: smtp_port (1-65535), smtp_mail_from (format email)"""
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
        """
        Charge la configuration depuis les variables Airflow

        Variables obligatoires:
            - universite
            - api_endpoint_admin
            - api_endpoint_table

        Variables optionnelles: voir docstring du module
        """

        def get_var(key: str, default=None, required: bool = False):
            """Helper pour récupérer une variable avec gestion d'erreur"""
            try:
                value = VarMgr.get(key, default=default)
                if value is None and required:
                    raise ValueError(f"Variable Airflow requise manquante: {key}")
                return value
            except Exception as e:
                if required:
                    raise ValueError(f"Variable Airflow requise manquante: {key}") from e
                return default

        # Parse les destinataires (séparés par virgule)
        recipients_str = get_var('amue_report_recipients', 'admin@example.com')
        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

        return cls(
            # Obligatoires
            universite=get_var('universite', required=True),
            api_endpoint_admin=get_var('api_endpoint_admin', required=True),
            api_endpoint_table=get_var('api_endpoint_table', required=True),

            # API
            api_max_retries=int(get_var('amue_api_max_retries', 3)),
            api_retry_delay_seconds=int(get_var('amue_api_retry_delay_seconds', 30)),

            # Polling
            polling_interval_minutes=int(get_var('amue_polling_interval_minutes', 10)),
            polling_max_wait_hours=int(get_var('amue_max_wait_hours', 6)),
            polling_exponential_backoff=get_var('amue_polling_exponential_backoff', 'false').lower() == 'true',

            # Import
            import_batch_size=int(get_var('amue_import_batch_size', 5000)),
            max_history_days=int(get_var('amue_max_history_days', 7)),

            # Email
            smtp_host=get_var('smtp_host', 'mailhog'),
            smtp_port=int(get_var('smtp_port', 1025)),
            smtp_from=get_var('smtp_mail_from', 'airflow@amue.local'),
            report_recipients=recipients,

            # Environnement
            environment=get_var('environment', 'production')
        )

    def is_production(self) -> bool:
        """Vérifie si environment == 'production'"""
        return self.environment.lower() == 'production'

    def is_development(self) -> bool:
        """Vérifie si environment == 'dev' ou 'development'"""
        return self.environment.lower() in ('dev', 'development')


# Instance globale (lazy loading)
_config: Optional[AMUEConfig] = None


def get_config() -> AMUEConfig:
    """Récupère la configuration globale (singleton)"""
    global _config
    if _config is None:
        _config = AMUEConfig.from_airflow_variables()
    return _config


def reload_config() -> AMUEConfig:
    """Force le rechargement de la configuration depuis Airflow"""
    global _config
    _config = AMUEConfig.from_airflow_variables()
    return _config
