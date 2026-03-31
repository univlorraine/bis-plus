"""
DAG de monitoring du statut API AMUE

Appelle l'API AMUE toutes les minutes et log le contenu complet de la réponse
dès qu'un changement est détecté. Tourne pendant 4 heures (22h → 2h).

================================================================================
FONCTIONNEMENT
================================================================================

Une seule task en boucle :
    - Appel API toutes les 60 secondes
    - Comparaison de la réponse brute avec la précédente (JSON sérialisé)
    - Log complet (JSON indenté) à chaque changement avec l'horodatage
    - Silencieux si pas de changement
    - S'arrête après 4h ou si la task est killée

================================================================================
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

_MONITOR_DURATION_HOURS = 4
_POLL_INTERVAL_SECONDS = 60

_schedule = VarMgr.get('amue_monitor_schedule', default='0 22 * * *')


@dag(
    dag_id='amue_status_monitor',
    description='Monitoring statut API AMUE — log les changements toutes les minutes pendant 4h',
    schedule=_schedule,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['amue', 'monitoring'],
    default_args={
        'owner': 'airflow',
        'retries': 0,
    },
)
def amue_status_monitor():
    @task(
        task_id='watch_status',
        execution_timeout=timedelta(hours=_MONITOR_DURATION_HOURS + 1),
        retries=0,
    )
    def watch_status():
        from amue.hooks.amue_api_hook import AMUEAPIHook
        from amue.services.api.status_checker import AMUEStatusChecker
        from amue.services.api.status_monitor import StatusMonitor

        checker = AMUEStatusChecker(api_hook=AMUEAPIHook())
        StatusMonitor(
            checker,
            duration_hours=_MONITOR_DURATION_HOURS,
            poll_interval_seconds=_POLL_INTERVAL_SECONDS,
        ).watch()

    watch_status()


amue_status_monitor_dag = amue_status_monitor()
