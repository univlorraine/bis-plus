"""
Layer: infrastructure

Configuration centralisée AMUE
Charge les paramètres depuis les variables Airflow avec validation

================================================================================
VARIABLES AIRFLOW UTILISÉES
================================================================================

OBLIGATOIRES :
--------------
  universite                    Code université (ex: "univ", "unistra")
                                Format: 2-50 caractères alphanumériques ou tirets

  api_endpoint_admin            Endpoint API admin avec placeholder $univ
                                Ex: "/sifacweb/admin/$univ/admin"

  api_endpoint_table            Endpoint API données avec placeholders $univ et $table
                                Ex: "/sifacweb/data/$univ/table"

OPTIONNELLES (avec valeurs par défaut) :
----------------------------------------
  [Tables] La liste des tables à importer n'est plus une variable Airflow.
           Elle est stockée dans la table PostgreSQL splus_admin.amue_tables
           et gérée via TableConfigManager (plugins/amue/services/table_config_manager.py).
           Utiliser `./manage.sh load-tables` pour la configurer.

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

  amue_tables_to_purge          Tables à purger (TRUNCATE) au début de chaque import
                                Format: JSON array de noms de tables
                                Ex: '["ztable1", "ztable2"]'
                                Défaut: '[]' (aucune purge)

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
import threading
from dataclasses import dataclass, field
from typing import List, Optional

from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr
from common.infrastructure.config.recipients import parse_recipients
from common.domain.protected_source import PROTECTED_SOURCE  # noqa: F401 — re-export pour rétro-compatibilité

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTES CENTRALISÉES
# =============================================================================

class AMUEDefaults:
    """
    Constantes par défaut centralisées pour tout le module AMUE.

    Utilisation:
        from amue.infrastructure.config.settings import AMUEDefaults
        batch_size = AMUEDefaults.IMPORT_BATCH_SIZE
    """

    # --- API ---
    API_MAX_RETRIES: int = 3
    API_RETRY_DELAY_SECONDS: int = 30
    API_TIMEOUT_SECONDS: int = 60
    API_CONNECTION_ID: str = "oauth_api"

    # --- Polling ---
    POLLING_INTERVAL_MINUTES: int = 10
    POLLING_MAX_WAIT_HOURS: int = 6
    POLLING_MAX_INTERVAL_MINUTES: int = 60
    POLLING_BACKOFF_FACTOR: float = 1.5

    # --- Import ---
    IMPORT_BATCH_SIZE: int = 5000
    IMPORT_BATCH_SIZE_MIN: int = 100
    IMPORT_BATCH_SIZE_MAX: int = 50000
    IMPORT_MAX_MEMORY_MB: int = 512
    IMPORT_LOG_EVERY_N_BATCHES: int = 10  # Log tous les N batchs
    IMPORT_MAX_PARALLEL_TABLES: int = 10

    # --- Database ---
    DB_CONNECTION_ID: str = "postgres_data"
    DB_SCHEMA: str = "splus"
    DB_SCHEMA_BLUE: str = "splus_blue"
    DB_SCHEMA_GREEN: str = "splus_green"
    DB_CONNECTION_TIMEOUT_SECONDS: int = 30

    # --- Blue/Green (toujours actif) ---
    BLUEGREEN_DEFAULT_ACTIVE: str = "blue"
    BLUEGREEN_LOCK_TIMEOUT_MINUTES: int = 120  # 2 heures max pour un import

    # --- Email ---
    SMTP_HOST: str = "mailhog"
    SMTP_PORT: int = 1025
    SMTP_FROM: str = "airflow@amue.local"
    SMTP_TIMEOUT_SECONDS: int = 30

    # --- Data ---
    DEFAULT_SOURCE: str = PROTECTED_SOURCE
    META_COLUMN_SOURCE: str = "_source"
    META_COLUMN_IMPORTED_AT: str = "_imported_at"

    # --- Logging ---
    LOG_PREFIX_IMPORT: str = "[IMPORT]"
    LOG_PREFIX_BATCH: str = "[BATCH]"
    LOG_PREFIX_API: str = "[API]"
    LOG_PREFIX_BLUEGREEN: str = "[BLUEGREEN]"
    LOG_PREFIX_SYNC: str = "[SYNC]"
    LOG_PREFIX_SWITCH: str = "[SWITCH]"

    # --- Validation ---
    TABLE_NAME_MAX_LENGTH: int = 63  # PostgreSQL limit
    COLUMN_NAME_MAX_LENGTH: int = 63
    UNIVERSITE_MIN_LENGTH: int = 2
    UNIVERSITE_MAX_LENGTH: int = 50

    @classmethod
    def calculate_batch_size(cls, column_count: int) -> int:
        """
        Calcule la taille de batch optimale selon le nombre de colonnes.

        Args:
            column_count: Nombre de colonnes dans la table

        Returns:
            Taille de batch recommandée
        """
        base_size = cls.IMPORT_BATCH_SIZE
        if column_count > 100:
            return max(cls.IMPORT_BATCH_SIZE_MIN, base_size // 4)
        elif column_count > 50:
            return max(cls.IMPORT_BATCH_SIZE_MIN, base_size // 2)
        return base_size

    @classmethod
    def get_log_prefix(cls, component: str) -> str:
        """Retourne le préfixe de log pour un composant"""
        prefixes = {
            'import': cls.LOG_PREFIX_IMPORT,
            'batch': cls.LOG_PREFIX_BATCH,
            'api': cls.LOG_PREFIX_API,
            'bluegreen': cls.LOG_PREFIX_BLUEGREEN,
            'sync': cls.LOG_PREFIX_SYNC,
            'switch': cls.LOG_PREFIX_SWITCH,
        }
        return prefixes.get(component.lower(), f"[{component.upper()}]")


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
    api_source: str = 'cdv'             # Variable: amue_api_source ('cdv' | 'entrepot')
    api_max_retries: int = AMUEDefaults.API_MAX_RETRIES
    api_retry_delay_seconds: int = AMUEDefaults.API_RETRY_DELAY_SECONDS

    # --- Polling ---
    polling_interval_minutes: int = AMUEDefaults.POLLING_INTERVAL_MINUTES
    polling_max_wait_hours: int = AMUEDefaults.POLLING_MAX_WAIT_HOURS
    polling_exponential_backoff: bool = False

    # --- Import ---
    import_batch_size: int = AMUEDefaults.IMPORT_BATCH_SIZE
    import_max_memory_mb: int = AMUEDefaults.IMPORT_MAX_MEMORY_MB

    # --- Email ---
    smtp_host: str = AMUEDefaults.SMTP_HOST
    smtp_port: int = AMUEDefaults.SMTP_PORT
    smtp_from: str = AMUEDefaults.SMTP_FROM
    report_recipients: List[str] = field(default_factory=lambda: ['admin@example.com'])

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
        recipients = parse_recipients(
            get_var('amue_report_recipients', 'admin@example.com'),
            warning_message=(
                "Variable Airflow 'amue_report_recipients' vide après parsing"
                " — aucun rapport AMUE ne sera envoyé par email"
            ),
        )

        return cls(
            # Obligatoires
            universite=get_var('universite', required=True),
            api_endpoint_admin=get_var('api_endpoint_admin', required=True),
            api_endpoint_table=get_var('api_endpoint_table', required=True),

            # API
            api_source=get_var('amue_api_source', 'cdv'),
            api_max_retries=VarMgr.get_int('amue_api_max_retries', AMUEDefaults.API_MAX_RETRIES),
            api_retry_delay_seconds=VarMgr.get_int('amue_api_retry_delay_seconds', AMUEDefaults.API_RETRY_DELAY_SECONDS),

            # Polling
            polling_interval_minutes=VarMgr.get_int('amue_polling_interval_minutes', AMUEDefaults.POLLING_INTERVAL_MINUTES),
            polling_max_wait_hours=VarMgr.get_int('amue_max_wait_hours', AMUEDefaults.POLLING_MAX_WAIT_HOURS),
            polling_exponential_backoff=get_var('amue_polling_exponential_backoff', 'false').lower() == 'true',

            # Import
            import_batch_size=VarMgr.get_int('amue_import_batch_size', AMUEDefaults.IMPORT_BATCH_SIZE),

            # Email
            smtp_host=get_var('smtp_host', AMUEDefaults.SMTP_HOST),
            smtp_port=VarMgr.get_int('smtp_port', AMUEDefaults.SMTP_PORT),
            smtp_from=get_var('smtp_mail_from', AMUEDefaults.SMTP_FROM),
            report_recipients=recipients,
        )


# Instance globale (lazy loading avec thread-safety)
_config: Optional[AMUEConfig] = None
_config_lock = threading.Lock()


def get_config() -> AMUEConfig:
    """Récupère la configuration globale (singleton thread-safe)"""
    global _config
    if _config is None:
        with _config_lock:
            # Double-check locking pattern
            if _config is None:
                _config = AMUEConfig.from_airflow_variables()
    return _config


def reload_config() -> AMUEConfig:
    """Force le rechargement de la configuration depuis Airflow (thread-safe)"""
    global _config
    with _config_lock:
        _config = AMUEConfig.from_airflow_variables()
    return _config
