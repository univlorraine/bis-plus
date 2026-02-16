"""
DAG d'import AMUE - Import automatisé de données financières universitaires

Ce DAG récupère les données depuis l'API AMUE (Agence de Mutualisation des
Universités et Établissements) et les importe dans une base PostgreSQL.

================================================================================
ARCHITECTURE EN 4 PHASES
================================================================================

PHASE 1 - INITIALISATION
    └── wait_for_api_and_select()
        • Attend que l'API AMUE soit disponible (polling avec backoff)
        • Vérifie que le traitement côté AMUE est terminé (variable 'finish')
        • Sélectionne les tables à importer selon la configuration
        • Retourne la liste des tables pour le traitement parallèle

PHASE 2 - VÉRIFICATION (parallèle, 1 task par table)
    ├── verify_table.expand()
    │   • Vérifie le statut de chaque table côté API
    │   • Compare le fingerprint pour détecter les changements de structure
    │   • Récupère la définition des colonnes
    │
    └── validate_tables()
        • Agrège les résultats de vérification
        • STOPPE le DAG si une table est en erreur (fail-fast)

PHASE 3 - IMPORT (parallèle, 1 task par table)
    ├── prepare_table.expand()
    │   • Crée/modifie la table PostgreSQL si nécessaire (dev uniquement)
    │   • En production : vérifie que la structure existe
    │
    └── import_data.expand()
        • Récupère les données par batch depuis l'API
        • INSERT ou UPSERT selon la présence de clé primaire
        • Gestion des erreurs avec retry intelligent

PHASE 4 - FINALISATION
    ├── save_metadata()
    │   • Met à jour les fingerprints dans les variables Airflow
    │   • Enregistre la date de dernier import par table
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

Variables principales :
    - amue_tables_to_import : Liste JSON des tables à importer
    - environment : "dev" ou "production"
    - universite : Code université (ex: "ul")

================================================================================
PLANIFICATION
================================================================================

Schedule : Tous les jours à 2h00 (0 2 * * *)
Catchup  : Désactivé (pas de rattrapage des exécutions manquées)
Max runs : 1 seul DAG run actif à la fois

================================================================================
"""
from datetime import datetime, timedelta
from airflow.sdk import dag

from amue import send_failure_notification
from amue.tasks.import_dag import (
    init_bluegreen,
    wait_for_api_and_select,
    verify_table,
    validate_tables,
    prepare_table,
    import_data,
    save_metadata,
    switch_views,
    send_report,
)


# ==============================================================================
# DÉFINITION DU DAG
# ==============================================================================

@dag(
    dag_id='amue_multi_table_import',
    description='Import AMUE - Architecture simplifiée',

    # --- Planification ---
    schedule='0 2 * * *',           # Tous les jours à 2h00
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # Pas de rattrapage des runs manqués
    max_active_runs=1,              # Un seul run actif à la fois

    # --- Métadonnées ---
    tags=['amue', 'production'],

    # --- Gestion des erreurs ---
    # Envoie un email en cas d'échec du DAG
    on_failure_callback=send_failure_notification,

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,               # Pas de retry automatique (géré dans le code)
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_notification,
    }
)
def amue_multi_table_import():
    """
    DAG principal d'import AMUE

    Workflow :
        tables = wait_for_api_and_select()
            ↓
        verifications = verify_table.expand(tables)  # Parallèle
            ↓
        validated = validate_tables(verifications)
            ↓
        prepared = prepare_table.expand(validated)   # Parallèle
            ↓
        imported = import_data.expand(prepared)      # Parallèle
            ↓
        save_metadata(imported) >> send_report(imported)
    """

    # ==========================================================================
    # WORKFLOW (enchaînement des tasks)
    # ==========================================================================

    # Phase 0 : Blue/Green Initialisation
    bluegreen_ctx = init_bluegreen()

    # Phase 1 : Initialisation
    tables = wait_for_api_and_select(bluegreen_ctx)

    # Phase 2 : Vérification
    verifications = verify_table.expand(table_info=tables)
    validated = validate_tables(verifications)

    # Phase 3 : Import
    prepared = prepare_table.expand(verified_table=validated)
    imported = import_data.expand(prepared_table=prepared)

    # Phase 4 : Finalisation
    metadata = save_metadata(imported)

    # Phase 5 : Blue/Green Switch
    switch_result = switch_views(metadata)

    # Phase 6 : Rapport
    report = send_report(imported, switch_result)


# ==============================================================================
# INSTANCIATION DU DAG
# ==============================================================================
amue_import_dag = amue_multi_table_import()
