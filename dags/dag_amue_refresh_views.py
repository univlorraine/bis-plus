"""
DAG de rafraîchissement des vues custom blue/green AMUE.

================================================================================
RÔLE
================================================================================

Recrée toutes les vues custom (scripts/sql/custom_views/*.sql) en les faisant
pointer vers le schéma actuellement actif (splus_blue ou splus_green).

Utile après :
    - l'ajout d'un nouveau fichier .sql dans custom_views/
    - la correction d'une vue cassée
    - une maintenance manuelle des vues

WORKFLOW :
    detect_active_schema()   — détermine splus_blue ou splus_green
        ↓
    refresh_custom_views()   — réexécute tous les .sql du répertoire
        ↓
    send_refresh_report()    — envoie le rapport par email

================================================================================
DÉCLENCHEMENT
================================================================================

Ce DAG est manuel uniquement (schedule=None).
Déclencher via l'interface Airflow ou :
    airflow dags trigger amue_refresh_views

================================================================================
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag

from amue import send_failure_notification
from amue.tasks.refresh_views_dag import (
    detect_active_schema,
    refresh_custom_views,
    send_refresh_report,
)


@dag(
    dag_id='amue_refresh_views',
    description='Rafraîchit les vues custom AMUE vers le schéma actif',

    # Déclenchement manuel uniquement
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'bluegreen', 'views'],

    on_failure_callback=send_failure_notification,

    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
)
def amue_refresh_views():
    """
    DAG de rafraîchissement des vues custom.

    Workflow :
        schema_info = detect_active_schema()
                        ↓
        result      = refresh_custom_views(schema_info)
                        ↓
        send_refresh_report(result)
    """
    schema_info = detect_active_schema()
    result = refresh_custom_views(schema_info)
    send_refresh_report(result)


refresh_views_dag = amue_refresh_views()
