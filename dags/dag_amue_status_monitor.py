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

import json
import logging
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)

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

        api_hook = AMUEAPIHook()
        checker = AMUEStatusChecker(api_hook=api_hook)

        deadline = datetime.now() + timedelta(hours=_MONITOR_DURATION_HOURS)
        previous_snapshot: str | None = None

        logger.info(
            f"[MONITOR] Démarrage — surveillance jusqu'à {deadline:%H:%M:%S} "
            f"(intervalle {_POLL_INTERVAL_SECONDS}s)"
        )

        while datetime.now() < deadline:
            now_str = datetime.now().strftime('%H:%M:%S')

            try:
                result = checker.fetch_full_status()
                raw = result.get('raw_response') or result
                snapshot = json.dumps(raw, sort_keys=True, default=str)

                if snapshot != previous_snapshot:
                    logger.info(
                        f"[MONITOR] {now_str} — CHANGEMENT DÉTECTÉ\n"
                        + json.dumps(raw, indent=2, default=str)
                    )
                    previous_snapshot = snapshot

            except Exception as exc:
                logger.warning(f"[MONITOR] {now_str} — Erreur API: {exc}")

            time.sleep(_POLL_INTERVAL_SECONDS)

        logger.info(f"[MONITOR] Fin de surveillance après {_MONITOR_DURATION_HOURS}h")

    watch_status()


amue_status_monitor_dag = amue_status_monitor()
