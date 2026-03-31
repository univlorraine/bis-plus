"""
Sensor Airflow pour attendre la disponibilité de l'API AMUE.

Ce sensor utilise le mode 'reschedule' pour libérer le worker entre
les vérifications (pokes), évitant de bloquer un slot worker pendant
l'attente.

Usage dans le DAG :
    wait_sensor = AMUEAPISensor(
        task_id='wait_for_api',
        poke_interval=600,   # Vérifie toutes les 10 minutes
        timeout=21600,       # Timeout après 6 heures
    )
"""
import logging
from datetime import datetime
from typing import Any

from airflow.sdk.bases.sensor import BaseSensorOperator

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.services.api.status_checker import AMUEStatusChecker
from amue.services.api.finish_timestamp_validator import FinishTimestampValidator
from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class AMUEAPISensor(BaseSensorOperator):
    """
    Sensor qui attend que l'API AMUE soit prête.

    Vérifie à intervalle régulier :
        1. Que l'API retourne HTTP 200
        2. Que le champ 'finish' est renseigné (traitement AMUE terminé)
        3. Que le timestamp 'finish' est nouveau par rapport au dernier import

    En mode 'reschedule', le worker est libéré entre les pokes.

    Attributes:
        poke_interval: Intervalle entre les vérifications (secondes)
        timeout: Timeout total (secondes)
        mode: 'reschedule' pour libérer le worker entre les pokes
    """

    def __init__(self, *, poke_interval: int = 600, timeout: int = 21600, **kwargs: Any):
        super().__init__(
            mode='reschedule',
            poke_interval=poke_interval,
            timeout=timeout,
            **kwargs
        )
        self._polling_result = None

    def poke(self, context: Any) -> bool:
        """
        Vérifie si l'API est prête.

        Returns:
            True si l'API est disponible et le traitement terminé.
        """
        logger.info("[SENSOR] Vérification de l'API AMUE...")

        try:
            api_hook = AMUEAPIHook()
            status_checker = AMUEStatusChecker(api_hook)
            full_status = status_checker.fetch_full_status()
        except Exception as e:
            logger.warning(f"[SENSOR] Erreur lors de la vérification: {e}")
            return False

        # Vérifie HTTP 200
        http_code = full_status.get('http_status', 0)
        if http_code != 200:
            logger.info(f"[SENSOR] API non disponible (HTTP {http_code})")
            return False

        # Vérifie que finish est renseigné
        finish = full_status.get('finish', '')
        if not finish:
            logger.info("[SENSOR] Traitement AMUE en cours (finish vide)")
            return False

        # Vérifie que le timestamp est nouveau (avec normalisation de format)
        force_import = VarMgr.get('amue_force_import', default='false').lower() == 'true'
        if force_import:
            logger.info("[SENSOR] Force import activé — contrainte finish ignorée")
        elif FinishTimestampValidator().should_skip(finish):
            logger.info(f"[SENSOR] Même timestamp finish ({finish}), en attente de nouveau traitement")
            return False

        # API prête - stocker le résultat pour execute()
        logger.info(f"[SENSOR] API prête ! finish={finish}")
        self._polling_result = {
            'http_code': http_code,
            'finish': finish,
            'report_start': full_status.get('start', '') or '',
            'tables_status': full_status.get('tables_status', {}),
            'start_time': datetime.now().isoformat(),
            'attempts': 1,
            'total_wait_minutes': 0,
        }
        context['ti'].xcom_push(key='polling_result', value=self._polling_result)

        return True

    def execute(self, context: Any) -> dict:
        """
        Exécute le sensor et retourne le polling_result.

        Override de BaseSensorOperator.execute() pour que le polling_result
        soit disponible via wait_sensor.output (XCom return_value).
        """
        super().execute(context)
        return self._polling_result
