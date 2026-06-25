"""
DAG de gestion des variables Airflow du projet (AMUE, ECC, SMTP).

================================================================================
RÔLE
================================================================================

Les variables Airflow (Admin > Variables) sont en texte libre : aucune
validation de forme n'y est appliquée. Ce DAG fournit un formulaire de
déclenchement unique pour modifier les variables listées dans
config/airflow_variables.json, avec :
    - menu déroulant pour les valeurs à choix limité (ex: amue_api_source)
    - cases à cocher pour les booléens
    - bornes min/max pour les entiers (alignées sur les validations de
      plugins/amue/utils/config/settings.py)

et écrit les valeurs choisies dans les Variables Airflow correspondantes.

La variable TYPE_MAPPING_SQLITE_TO_POSTGRES n'est pas exposée ici : c'est
une table de correspondance structurelle, pas un réglage opérationnel.

================================================================================
DÉCLENCHEMENT
================================================================================

  1. Ouvrir le DAG 'amue_settings' dans l'UI Airflow
  2. Cliquer sur 'Trigger DAG w/ config'
  3. Modifier les valeurs souhaitées (les autres restent inchangées)
  4. Soumettre

Une valeur hors des bornes/enum est rejetée par Airflow avant même la
création du DagRun (validation JSON-Schema côté serveur).

================================================================================
"""
import json
import logging
import re
from pathlib import Path

from airflow.models.param import Param
from airflow.sdk import dag, task

from amue.infrastructure.notifications import send_failure_notification
from common.dags import DEFAULT_START_DATE, standard_default_args
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

_log = logging.getLogger(__name__)

_DAG_ID_PATTERN = re.compile(r"dag_id\s*=\s*['\"]([a-zA-Z0-9_.-]+)['\"]")


def _list_known_dag_ids(exclude: set) -> list:
    """
    Liste les dag_id existants en lisant les fichiers de dags/ (texte brut).

    Volontairement pas de requête DB/API : en Airflow 3.x, le parsing d'un
    DAG ne peut pas accéder à la base de métadonnées (ORM bloqué) et une
    liste de tous les DagBag voudrait re-parser tout le dossier. La lecture
    de fichier reste sûre et rapide au parsing.
    """
    dag_ids = set()
    try:
        for py_file in Path(__file__).parent.glob('*.py'):
            try:
                text = py_file.read_text(encoding='utf-8')
            except OSError:
                continue
            dag_ids.update(_DAG_ID_PATTERN.findall(text))
    except Exception as exc:
        _log.warning(f"[SETTINGS] Liste des dag_id indisponible au parsing: {type(exc).__name__}: {exc}")
    return sorted(dag_ids - exclude)


_KNOWN_DAG_IDS = _list_known_dag_ids(exclude={'amue_settings'})


def _current_value(key: str, default):
    """Lit la valeur actuelle d'une Variable Airflow pour pré-remplir le Param du formulaire."""
    try:
        raw = VarMgr.get(key, default=None)
        if raw is None:
            return default
        if isinstance(default, bool):
            return str(raw).strip().lower() in ('1', 'true', 'yes')
        if isinstance(default, int):
            return int(raw)
        if isinstance(default, list):
            return json.loads(raw) if raw else default
        return raw
    except Exception as exc:
        _log.warning(f"[SETTINGS] Lecture Variable '{key}' impossible au parsing: {type(exc).__name__}: {exc}")
        return default


def _v(key: str, default):
    """Raccourci: valeur courante de `key`, repliée sur `default`."""
    return _current_value(key, default)


@dag(
    dag_id='amue_settings',
    description='Modification contrôlée des variables Airflow du projet',

    schedule=None,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'settings'],

    params={
        # --- Planification (expressions cron) ---
        'amue_import_schedule': Param(
            default=_v('amue_import_schedule', '0 2 * * *'),
            type='string',
            description="Planification de l'import AMUE (cron).",
        ),
        'amue_sync_schedule': Param(
            default=_v('amue_sync_schedule', '0 6 * * *'),
            type='string',
            description='Planification de la synchronisation blue/green (cron).',
        ),
        'amue_monitor_schedule': Param(
            default=_v('amue_monitor_schedule', '0 22 * * *'),
            type='string',
            description='Planification du monitoring de statut AMUE (cron).',
        ),
        'ecc_import_schedule': Param(
            default=_v('ecc_import_schedule', '0 4 * * *'),
            type='string',
            description="Planification de l'import ECC (cron).",
        ),

        # --- Université / API AMUE ---
        'universite': Param(
            default=_v('universite', 'ul'),
            type='string',
            minLength=2,
            maxLength=50,
            pattern='^[a-zA-Z0-9-]{2,50}$',
            description='Code université (2-50 caractères alphanumériques ou tirets).',
        ),
        'amue_api_source': Param(
            default=_v('amue_api_source', 'entrepot'),
            type='string',
            enum=['cdv', 'entrepot'],
            description="Source de l'API AMUE active.",
        ),
        'api_endpoint_admin': Param(
            default=_v('api_endpoint_admin', ''),
            type='string',
            description="Endpoint API admin (CDV) avec placeholder ${univ}.",
        ),
        'api_endpoint_table': Param(
            default=_v('api_endpoint_table', ''),
            type='string',
            description="Endpoint API données (CDV) avec placeholder ${univ}.",
        ),
        'api_endpoint_entrepot': Param(
            default=_v('api_endpoint_entrepot', ''),
            type='string',
            description="Endpoint de base pour la source 'entrepot' avec placeholder ${univ}.",
        ),

        # --- Import AMUE ---
        'amue_import_batch_size': Param(
            default=_v('amue_import_batch_size', 5000),
            type='integer',
            minimum=100,
            maximum=50000,
            description='Nombre de lignes par batch d\'import AMUE (100-50000).',
        ),
        'amue_import_parallel_workers': Param(
            default=_v('amue_import_parallel_workers', 1),
            type='integer',
            minimum=1,
            maximum=10,
            description="Nombre de workers parallèles pour l'import AMUE (1-10).",
        ),
        'amue_api_max_retries': Param(
            default=_v('amue_api_max_retries', 3),
            type='integer',
            minimum=1,
            maximum=10,
            description="Nombre max de tentatives API AMUE en cas d'erreur (1-10).",
        ),
        'amue_api_retry_delay_seconds': Param(
            default=_v('amue_api_retry_delay_seconds', 30),
            type='integer',
            minimum=1,
            maximum=300,
            description='Délai entre tentatives API AMUE, en secondes (1-300).',
        ),
        'amue_force_import': Param(
            default=_v('amue_force_import', False),
            type='boolean',
            description="Force l'import même si le statut API ne l'autorise pas.",
        ),

        # --- Polling AMUE ---
        'amue_polling_interval_minutes': Param(
            default=_v('amue_polling_interval_minutes', 10),
            type='integer',
            minimum=1,
            maximum=120,
            description='Intervalle entre vérifications de disponibilité API, en minutes (1-120).',
        ),
        'amue_max_wait_hours': Param(
            default=_v('amue_max_wait_hours', 6),
            type='integer',
            minimum=1,
            maximum=24,
            description="Durée max d'attente de l'API AMUE, en heures (1-24).",
        ),
        'amue_polling_exponential_backoff': Param(
            default=_v('amue_polling_exponential_backoff', False),
            type='boolean',
            description='Active le backoff exponentiel pour le polling AMUE.',
        ),
        'amue_polling_max_backoff_minutes': Param(
            default=_v('amue_polling_max_backoff_minutes', 60),
            type='integer',
            minimum=1,
            maximum=1440,
            description='Intervalle de polling maximum avec backoff, en minutes (1-1440).',
        ),

        # --- Pré/post-import (DAGs déclenchés en chaîne) ---
        'amue_pre_import_dags': Param(
            default=_v('amue_pre_import_dags', []),
            type='array',
            examples=_KNOWN_DAG_IDS,
            description="Liste des dag_id à déclencher avant l'import AMUE.",
        ),
        'amue_post_import_dags': Param(
            default=_v('amue_post_import_dags', []),
            type='array',
            examples=_KNOWN_DAG_IDS,
            description="Liste des dag_id à déclencher après l'import AMUE.",
        ),

        # --- ECC ---
        'ecc_import_batch_size': Param(
            default=_v('ecc_import_batch_size', 5000),
            type='integer',
            minimum=100,
            maximum=50000,
            description="Nombre de lignes par batch d'import ECC (100-50000).",
        ),

        # --- Rapports ---
        'amue_reports_dir': Param(
            default=_v('amue_reports_dir', '/opt/airflow/logs/reports'),
            type='string',
            description='Répertoire de sortie des rapports AMUE.',
        ),
        'amue_report_recipients': Param(
            default=_v('amue_report_recipients', ''),
            type='string',
            description='Destinataires des rapports AMUE (emails séparés par virgule).',
        ),
        'ecc_report_recipients': Param(
            default=_v('ecc_report_recipients', ''),
            type='string',
            description='Destinataires des rapports ECC (emails séparés par virgule).',
        ),

        # --- SMTP ---
        'smtp_host': Param(
            default=_v('smtp_host', 'mailhog'),
            type='string',
            description='Serveur SMTP pour les notifications.',
        ),
        'smtp_port': Param(
            default=_v('smtp_port', 1025),
            type='integer',
            minimum=1,
            maximum=65535,
            description='Port SMTP (1-65535).',
        ),
        'smtp_use_tls': Param(
            default=_v('smtp_use_tls', False),
            type='boolean',
            description='Active TLS pour la connexion SMTP.',
        ),
        'smtp_timeout': Param(
            default=_v('smtp_timeout', 30),
            type='integer',
            minimum=1,
            maximum=300,
            description='Timeout de connexion SMTP, en secondes (1-300).',
        ),
        'smtp_mail_from': Param(
            default=_v('smtp_mail_from', 'airflow@amue.local'),
            type='string',
            format='email',
            description="Adresse expéditeur des emails (format email).",
        ),
        'smtp_sender_name': Param(
            default=_v('smtp_sender_name', 'Airflow'),
            type='string',
            description="Nom d'expéditeur affiché dans les emails.",
        ),
    },

    on_failure_callback=send_failure_notification,
    default_args=standard_default_args(),
)
def amue_settings():
    """
    Workflow :
        apply_settings()   ← écrit chaque param validé dans la Variable Airflow correspondante
    """

    @task(task_id='apply_settings')
    def apply_settings(**context):
        params = context['params']
        for key, value in params.items():
            VarMgr.set(key, value)
        return params

    apply_settings()


amue_settings_dag = amue_settings()
