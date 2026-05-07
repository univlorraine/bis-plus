"""
DAG de rollback blue/green AMUE.

================================================================================
RÔLE
================================================================================

Permet de revenir au schéma précédent après un import raté ou indésirable.
Le rollback est disponible tant que le schéma offline (splus_X_offline) existe,
soit jusqu'au prochain import réussi (qui écrase l'offline).

WORKFLOW :
    check_rollback()        — vérifie qu'un schéma offline est disponible
        ↓
    perform_rollback()      — restore offline → switch vues → met actif en offline
        ↓
    send_rollback_report()  — envoie le rapport par email

================================================================================
DÉCLENCHEMENT
================================================================================

Ce DAG est manuel uniquement (schedule=None).
Déclencher via l'interface Airflow ou :
    airflow dags trigger amue_rollback

================================================================================
"""
from airflow.sdk import dag

from amue.notifications import send_failure_notification
from amue.tasks.rollback_dag import check_rollback, perform_rollback, send_rollback_report
from common.dags import DEFAULT_START_DATE, standard_default_args


@dag(
    dag_id='amue_rollback',
    description='Rollback blue/green AMUE — restaure le schéma précédent',

    # Déclenchement manuel uniquement
    schedule=None,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'rollback', 'bluegreen'],

    on_failure_callback=send_failure_notification,

    default_args=standard_default_args(),
)
def amue_rollback():
    """
    DAG de rollback blue/green.

    Workflow :
        check  = check_rollback()
                    ↓
        result = perform_rollback(check)
                    ↓
        send_rollback_report(result)
    """
    check = check_rollback()
    result = perform_rollback(check)
    send_rollback_report(result)


rollback_dag = amue_rollback()
