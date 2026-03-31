"""
DAG de synchronisation blue/green AMUE

Ce DAG copie le contenu du schéma blue/green actif vers le schéma inactif,
afin de les maintenir à parité après chaque import.

================================================================================
FONCTIONNEMENT
================================================================================

PHASE 1 - INIT
    └── init_sync()
        • Vérifie que le mode blue/green est activé
        • Détermine le schéma source (actif) et la cible (inactif)

PHASE 2 - SYNCHRONISATION
    └── run_sync()
        • Copie toutes les tables : TRUNCATE + INSERT (commit par table)
        • Enregistre last_sync_timestamp en BDD (splus_admin.amue_state)

PHASE 3 - RAPPORT
    └── send_sync_report()
        • Envoie un email récapitulatif (tables, lignes copiées, erreurs)

================================================================================
DÉCLENCHEMENT
================================================================================

Schedule configurable via la variable Airflow 'amue_sync_schedule'.
Valeur par défaut : '0 6 * * *' (6h00 chaque jour).
Le DAG peut aussi être déclenché manuellement.

================================================================================
"""
from datetime import datetime, timedelta

from airflow.sdk import dag

from amue import send_failure_notification
from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.tasks.sync_dag import init_sync, run_sync, send_sync_report


# Schedule configurable via variable Airflow
_sync_schedule = VarMgr.get('amue_sync_schedule', default='0 6 * * *')


@dag(
    dag_id='amue_sync_schemas',
    description='Synchronisation blue/green AMUE (copie schéma actif → inactif)',

    schedule=_sync_schedule,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'bluegreen', 'sync'],

    on_failure_callback=send_failure_notification,

    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
)
def amue_sync_schemas():
    """
    DAG de synchronisation des schémas blue/green.

    Workflow :
        init_sync()
            ↓
        run_sync(ctx)
            ↓
        send_sync_report(result)
    """
    ctx = init_sync()
    result = run_sync(ctx)
    send_sync_report(result)


amue_sync_dag = amue_sync_schemas()
