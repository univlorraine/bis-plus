"""
DAG de Rollback Blue/Green pour l'import AMUE

Ce DAG permet de revenir rapidement à l'état précédent en cas de
problème avec les données importées. Le rollback est instantané car
il consiste simplement à reswitcher les vues vers l'ancien schéma.

================================================================================
QUAND UTILISER CE DAG
================================================================================

Utilisez ce DAG quand :
    - Les données importées sont incorrectes
    - Un problème est détecté après un import réussi
    - Vous souhaitez revenir à l'état précédent

CONDITIONS :
    - Le mode blue/green doit être activé
    - Un import doit avoir été effectué depuis la dernière sync
    - Aucun import ne doit être en cours

================================================================================
COMMENT DÉCLENCHER
================================================================================

1. Via Airflow CLI :
   airflow dags trigger amue_rollback

2. Via API Airflow :
   POST /api/v1/dags/amue_rollback/dagRuns

3. Via UI Airflow :
   Aller dans DAGs > amue_rollback > Trigger DAG

================================================================================
"""
from datetime import datetime
from airflow.sdk import dag

from amue import send_failure_notification
from amue.tasks.rollback_dag import (
    check_rollback_available,
    preview_rollback,
    execute_rollback,
    verify_rollback,
)


@dag(
    dag_id='amue_rollback',
    description='Rollback Blue/Green - Retour à l\'état précédent',

    # --- Planification ---
    schedule=None,                  # Déclenché manuellement uniquement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,              # Un seul rollback à la fois

    # --- Métadonnées ---
    tags=['amue', 'bluegreen', 'rollback', 'manual'],

    # --- Gestion des erreurs ---
    on_failure_callback=send_failure_notification,

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'on_failure_callback': send_failure_notification,
    }
)
def amue_rollback():
    """
    DAG de rollback blue/green

    Workflow :
        check_rollback_available()
            ↓
        preview_rollback()
            ↓
        execute_rollback()
            ↓
        verify_rollback()
    """

    # Workflow
    check = check_rollback_available()
    preview = preview_rollback(check)
    rollback = execute_rollback(preview)
    verify = verify_rollback(rollback)


# Instanciation du DAG
amue_rollback_dag = amue_rollback()
