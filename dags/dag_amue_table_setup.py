"""
DAG de setup des tables AMUE

Ce DAG initialise et maintient la structure des tables AMUE dans PostgreSQL.
Il prend en charge tout ce qui est "one-time" ou "sur changement" :
    - Création des tables dans le schéma blue/green cible
    - Calcul et sauvegarde des fingerprints (structure API + structure PG)
    - Récupération et sauvegarde des clés primaires
    - Détection des changements de structure (avec alerte, sans modification automatique)

================================================================================
MODES D'EXÉCUTION
================================================================================

MODE STANDALONE (manuel ou planifié) :
    - Détecter le schéma cible via BlueGreenManager
    - Traiter toutes les tables enabled dans splus_admin.amue_tables

MODE DÉCLENCHÉ PAR LA DAG PRINCIPALE :
    - Reçoit target_schema via conf du TriggerDagRunOperator
    - Même comportement, schéma cible imposé

================================================================================
WORKFLOW
================================================================================

    select_setup_tables(conf)
        ↓  (liste des tables enabled, enrichies avec target_schema)
    setup_table.expand(table_info=tables)
        ↓  (par table : verify + create si absent + save fingerprints/PKs)
    send_setup_report(results)
        ↓  (log + email si tables bloquées)

================================================================================
STATUTS
================================================================================

    pending  → jamais initialisé (défaut à la création)
    ready    → initialisé avec succès, prêt pour l'import
    blocked  → changement de structure détecté, intervention manuelle requise

================================================================================
"""
from datetime import datetime, timedelta

from airflow.sdk import dag

from amue import send_failure_notification
from amue.tasks.setup_dag import select_setup_tables, setup_table, send_setup_report


# ==============================================================================
# DÉFINITION DU DAG
# ==============================================================================

@dag(
    dag_id='amue_table_setup',
    description='Setup AMUE — initialisation et vérification des tables',

    # Pas de schedule : déclenché manuellement ou par la DAG principale
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'setup'],

    on_failure_callback=send_failure_notification,

    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
)
def amue_table_setup():
    """
    DAG de setup des tables AMUE

    Workflow :
        select_setup_tables(conf)
            ↓
        setup_table.expand(table_info=tables)
            ↓
        send_setup_report(results)
    """
    from airflow.sdk import task, get_current_context

    @task(task_id='read_conf')
    def read_conf() -> dict:
        """Lit la configuration du dag_run (target_schema, etc.)."""
        try:
            ctx = get_current_context()
            dag_run = ctx.get('dag_run')
            return dag_run.conf if dag_run and dag_run.conf else {}
        except Exception:
            return {}

    conf = read_conf()
    tables = select_setup_tables(conf)
    results = setup_table.expand(table_info=tables)
    send_setup_report(results)


# ==============================================================================
# INSTANCIATION
# ==============================================================================
amue_setup_dag = amue_table_setup()
