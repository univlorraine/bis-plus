"""
DAG d'import ECC — Oracle SAP ECC → PostgreSQL avec protection sifac_plus

================================================================================
ARCHITECTURE EN 4 PHASES
================================================================================

PHASE 1 - SÉLECTION
    └── select_ecc_tables()
        • Lit splus_admin.amue_tables (tables activées, ecc_query non-NULL)
        • Détermine le schéma inactif via BlueGreenManager
        • ECC insère dans le schéma inactif uniquement

PHASE 2 - IMPORT PARALLÈLE
    └── import_ecc_data.expand()
        • Oracle SQL → UPSERT PostgreSQL dans l'inactif (1 task par table)
        • Protection sifac_plus : les lignes _source='sifac_plus' ne sont pas
          remplacées (guard WHERE dans le DO UPDATE SET)

PHASE 3 - SYNCHRONISATION VERS L'ACTIF
    └── sync_ecc_to_active()
        • UPSERT inactif → actif (lignes ECC uniquement)
        • Transaction unique : si erreur, actif inchangé
        • Bloquante : save_ecc_metadata attend la fin de cette tâche

PHASE 4 - RAPPORT
    └── send_ecc_report()
        • Génère et envoie le rapport d'import ECC

================================================================================
CONFIGURATION (Variables Airflow)
================================================================================

  ecc_import_schedule     Schedule cron (défaut: '0 4 * * *')
  ecc_import_batch_size   Taille batch (défaut: 5000)
  ecc_report_recipients   Destinataires emails (séparés par virgule)

Tables configurées dans : splus_admin.ecc_tables

================================================================================
"""
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sdk import dag

from ecc.notifications import send_ecc_failure_notification
from ecc.tasks.import_dag import select_ecc_tables, import_ecc_data, sync_ecc_to_active, save_ecc_metadata, send_ecc_report


@dag(
    dag_id='ecc_multi_table_import',
    description='Import ECC Oracle SAP → PostgreSQL avec protection sifac_plus',

    # --- Planification (configurable via Variable Airflow 'ecc_import_schedule') ---
    schedule=Variable.get('ecc_import_schedule', default_var=None),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,

    # --- Métadonnées ---
    tags=['ecc', 'production'],

    # --- Gestion des erreurs ---
    on_failure_callback=send_ecc_failure_notification,

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
)
def ecc_multi_table_import():
    """
    DAG d'import ECC Oracle → PostgreSQL.

    ECC insère dans le schéma inactif, puis synchronise vers l'actif.
    En cas d'échec, le schéma actif reste inchangé.

    Workflow :
        tables = select_ecc_tables()              ← inactif uniquement
            ↓
        imported = import_ecc_data.expand(...)    ← inactif
            ↓
        synced = sync_ecc_to_active(imported)     ← inactif → actif
            ↓
        saved = save_ecc_metadata.expand(...)
            ↓
        send_ecc_report(imported)
    """

    # ── Phase 1 : Sélection des tables ECC (+ détection schéma inactif) ──────
    tables = select_ecc_tables()

    # ── Phase 2 : Import parallèle Oracle → PostgreSQL (schéma inactif) ──────
    imported = import_ecc_data.expand(table_config=tables)

    # ── Phase 3 : Synchronisation inactif → actif (transaction unique) ───────
    synced = sync_ecc_to_active(imported)

    # ── Phase 4 : Sauvegarde des métadonnées (après sync réussie) ─────────────
    saved = save_ecc_metadata.expand(import_result=imported)
    synced >> saved

    # ── Phase 5 : Rapport (après completion des métadonnées) ──────────────────
    report = send_ecc_report(imported)
    saved >> report


# ==============================================================================
# INSTANCIATION DU DAG
# ==============================================================================
ecc_import_dag = ecc_multi_table_import()
