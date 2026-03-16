"""
DAG d'import ECC — Oracle SAP ECC → PostgreSQL avec protection sifac_plus

================================================================================
ARCHITECTURE EN 3 PHASES
================================================================================

PHASE 1 - SÉLECTION
    └── select_ecc_tables()
        • Lit splus_admin.ecc_tables (tables activées)
        • Détermine le schéma actif via BlueGreenManager
        • ECC insère dans le schéma actif — aucun switch de schéma

PHASE 2 - IMPORT PARALLÈLE
    └── import_ecc_data.expand()
        • Oracle SQL → UPSERT PostgreSQL (1 task par table)
        • Protection sifac_plus : les lignes _source='sifac_plus' ne sont pas
          remplacées (guard WHERE dans le DO UPDATE SET)

PHASE 3 - RAPPORT
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

from airflow.sdk import dag

from amue import send_failure_notification
from ecc.tasks.import_dag import select_ecc_tables, import_ecc_data, save_ecc_metadata, send_ecc_report


@dag(
    dag_id='ecc_multi_table_import',
    description='Import ECC Oracle SAP → PostgreSQL avec protection sifac_plus',

    # --- Planification ---
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,

    # --- Métadonnées ---
    tags=['ecc', 'production'],

    # --- Gestion des erreurs ---
    on_failure_callback=send_failure_notification,

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

    ECC insère directement dans le schéma actif (pas de switch blue/green).

    Workflow :
        tables = select_ecc_tables()
            ↓
        imported = import_ecc_data.expand(table_config=tables)
            ↓
        metadata = save_ecc_metadata.expand(import_result=imported)
            ↓
        send_ecc_report(imported)
    """

    # ── Phase 1 : Sélection des tables ECC (+ détection schéma actif) ────────
    tables = select_ecc_tables()

    # ── Phase 2 : Import parallèle Oracle → PostgreSQL ────────────────────────
    imported = import_ecc_data.expand(table_config=tables)

    # ── Phase 3 : Sauvegarde des métadonnées (audit trail par table) ──────────
    saved = save_ecc_metadata.expand(import_result=imported)

    # ── Phase 4 : Rapport (après completion des métadonnées) ──────────────────
    report = send_ecc_report(imported)
    saved >> report


# ==============================================================================
# INSTANCIATION DU DAG
# ==============================================================================
ecc_import_dag = ecc_multi_table_import()
