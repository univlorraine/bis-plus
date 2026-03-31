"""
DAG d'import AMUE - Import automatisé de données financières universitaires

Ce DAG récupère les données depuis l'API AMUE (Agence de Mutualisation des
Universités et Établissements) et les importe dans une base PostgreSQL.

================================================================================
ARCHITECTURE EN 5 PHASES
================================================================================

PHASE 0 - BLUE/GREEN INIT
    └── init_bluegreen()
        • Détermine le schéma cible (opposé de l'actif)
        • Acquiert le verrou d'import

PHASE 1 - POLLING & SÉLECTION
    ├── AMUEAPISensor (wait_for_api)
    │   • Attend que l'API AMUE soit disponible (mode reschedule)
    │   • Vérifie que le traitement côté AMUE est terminé
    │   • Push polling_result en XCom
    │
    └── select_tables()
        • Sélectionne les tables selon la configuration
        • Injecte le schéma cible blue/green

PHASE 2 - SETUP (délégué à amue_table_setup)
    └── TriggerDagRunOperator → amue_table_setup
        • Crée les tables si absentes
        • Vérifie/sauvegarde les fingerprints
        • Détecte les changements de structure

PHASE 3 - VALIDATION & IMPORT (parallèle, 1 task par table)
    ├── check_setup_status()
    │   • Lit setup_status depuis splus_admin.amue_tables
    │   • STOPPE si une table est 'pending' ou 'blocked'
    │
    └── import_data.expand()
        • Récupère les données par batch depuis l'API (colonnes lues depuis information_schema)
        • UPSERT avec gestion des erreurs et retry

PHASE 4 - FINALISATION
    ├── save_metadata()
    │   • Sauvegarde les timestamps (finish, report_start, last_success)
    │   • Les fingerprints sont gérés par amue_table_setup
    │
    ├── switch_views()
    │   • Bascule atomique des vues vers le schéma cible (blue/green)
    │
    └── send_report()
        • Génère un rapport HTML de l'exécution
        • Envoie par email aux destinataires configurés

================================================================================
CONFIGURATION (Variables Airflow)
================================================================================

Voir plugins/amue/utils/config/settings.py pour la liste complète des variables.

================================================================================
PLANIFICATION
================================================================================

Schedule : Configurable via variable Airflow 'amue_import_schedule' (défaut: '0 3 * * *')
Catchup  : Désactivé (pas de rattrapage des exécutions manquées)
Max runs : 1 seul DAG run actif à la fois

================================================================================
"""
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from amue import send_failure_notification, dag_failure_rollback
from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.config.settings import Defaults
from amue.sensors.amue_api_sensor import AMUEAPISensor
from amue.tasks.import_dag import (
    init_bluegreen,
    select_tables,
    check_setup_status,
    import_data,
    save_metadata,
    switch_views,
    send_report,
)
from common.config import PROTECTED_SOURCE
from common.tasks.restore_inactive import restore_inactive


# ==============================================================================
# DÉFINITION DU DAG
# ==============================================================================

# Schedule configurable via variable Airflow
_import_schedule = VarMgr.get('amue_import_schedule', default='0 3 * * *')

# Sensor configurable via variables Airflow
_sensor_poke_interval = int(VarMgr.get('amue_polling_interval_minutes',
                                        Defaults.POLLING_INTERVAL_MINUTES)) * 60
_sensor_timeout = int(VarMgr.get('amue_max_wait_hours',
                                  Defaults.POLLING_MAX_WAIT_HOURS)) * 3600


@dag(
    dag_id='amue_multi_table_import',
    description='Import AMUE - Architecture simplifiée',

    # --- Planification ---
    schedule=_import_schedule,      # Configurable via amue_import_schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # Pas de rattrapage des runs manqués
    max_active_runs=1,              # Un seul run actif à la fois

    # --- Métadonnées ---
    tags=['amue', 'production'],

    # --- Gestion des erreurs ---
    on_failure_callback=dag_failure_rollback,   # Rollback blue/green uniquement (pas d'email)

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,               # Pas de retry automatique (géré dans le code)
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_notification,  # Email avec vraie exception au niveau task
    }
)
def amue_multi_table_import():
    """
    DAG principal d'import AMUE

    Workflow :
        init_bluegreen()
            ↓
        AMUEAPISensor (wait_for_api)
            ↓
        tables = select_tables(bluegreen_ctx)
            ↓
        trigger_table_setup (amue_table_setup)
            ↓
        checked = check_setup_status(tables)
            ↓
        imported = import_data.expand(table_info=checked)
            ↓
        save_metadata(imported, polling_result)
            ↓
        switch_views(metadata)
            ↓
        send_report(imported, switch_result, polling_result)
    """

    # ==========================================================================
    # WORKFLOW (enchaînement des tasks)
    # ==========================================================================

    # Phase 0 : Blue/Green Initialisation
    bluegreen_ctx = init_bluegreen()

    # Phase 1 : Sensor polling (mode reschedule - libère le worker)
    wait_sensor = AMUEAPISensor(
        task_id='wait_for_api',
        poke_interval=_sensor_poke_interval,
        timeout=_sensor_timeout,
    )
    bluegreen_ctx >> wait_sensor

    # Phase 1b : Sélection des tables
    tables = select_tables(bluegreen_ctx)
    wait_sensor >> tables

    # Phase 2 : Setup — déclenche la DAG amue_table_setup avec le schéma cible
    trigger_setup = TriggerDagRunOperator(
        task_id='trigger_table_setup',
        trigger_dag_id='amue_table_setup',
        conf={"target_schema": "{{ ti.xcom_pull('init_bluegreen')['target_schema'] }}"},
        wait_for_completion=True,
        deferrable=True,
    )
    tables >> trigger_setup

    # Phase 3a : Validation du statut de setup (fail-fast si pending/blocked)
    checked = check_setup_status(tables)
    trigger_setup >> checked

    # Phase 3b : Import des données (colonnes lues depuis information_schema)
    imported = import_data.expand(table_info=checked)

    # Phase 3c : Restauration inactif sur échec (ALL_DONE, si ≥1 import raté)
    restore = restore_inactive(tables=checked, source_name=PROTECTED_SOURCE, import_results=imported)

    # Phase 4 : Finalisation - polling_result via XCom du sensor
    metadata = save_metadata(imported, wait_sensor.output)

    # Phase 5 : Blue/Green Switch
    switch_result = switch_views(metadata)

    # Phase 6 : Rapport
    send_report(imported, switch_result, wait_sensor.output)


# ==============================================================================
# INSTANCIATION DU DAG
# ==============================================================================
amue_import_dag = amue_multi_table_import()
