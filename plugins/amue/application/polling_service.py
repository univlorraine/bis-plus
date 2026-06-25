"""
Layer: application

Service de polling intelligent pour l'API AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module attend que l'API AMUE soit disponible et que le traitement côté
AMUE soit terminé avant de démarrer l'import. C'est le "gardien" du DAG
qui empêche de commencer l'import tant que les données ne sont pas prêtes.

CONDITIONS DE DISPONIBILITÉ :
    1. Code HTTP 200 (API accessible)
    2. Variable 'finish' renseignée dans la réponse JSON
       (indique que le traitement AMUE est terminé)

================================================================================
ARCHITECTURE INTERNE (après refactorisation)
================================================================================

AMUEPollingService compose :
    - FinishTimestampValidator   : validation du timestamp finish + should_skip
    - PollingStrategyCalculator  : calcul du nombre d'attempts et du wait_time

================================================================================
CONFIGURATION
================================================================================

Variables Airflow :
    - amue_polling_interval_minutes : Intervalle entre vérifications (défaut: 10)
    - amue_max_wait_hours : Durée max d'attente (défaut: 6)
    - amue_polling_exponential_backoff : Active le backoff (défaut: false)
    - amue_polling_max_backoff_minutes : Intervalle max en backoff (défaut: 60)

================================================================================
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.domain.finish_timestamp_validator import FinishTimestampValidator
from amue.domain.polling_strategy_calculator import PollingStrategyCalculator

logger = logging.getLogger(__name__)


@dataclass
class PollingConfig:
    """Configuration du service de polling."""
    interval_minutes: int
    max_wait_hours: int
    exponential_backoff: bool = False
    max_backoff_minutes: int = 60


@dataclass
class PollingResult:
    """Résultat d'une opération de polling."""
    ready: bool
    attempts: int
    total_wait_minutes: float
    status: str
    last_status_code: Optional[int] = None
    error: Optional[str] = None


class AMUEPollingService:
    """
    Service de polling pour attendre la disponibilité de l'API AMUE.

    Compose FinishTimestampValidator et PollingStrategyCalculator
    pour orchestrer la boucle de polling.

    Example:
        >>> service = AMUEPollingService(status_checker)
        >>> result = service.wait_for_ready()
    """

    def __init__(self, status_checker, config: Optional[PollingConfig] = None):
        """
        Args:
            status_checker: Instance de AMUEStatusChecker
            config: Configuration personnalisée (optionnelle)
        """
        self.status_checker = status_checker
        self.config = config or self._load_default_config()
        self.start_time = None
        self._cached_tables_status = None
        self._cached_report_start = None

        # Sous-composants
        self._ts_validator = FinishTimestampValidator()
        self._strategy = PollingStrategyCalculator(self.config)

    def _load_default_config(self) -> PollingConfig:
        """Charge la configuration depuis les variables Airflow."""
        return PollingConfig(
            interval_minutes=VarMgr.get_int('amue_polling_interval_minutes', default=10, min_value=1),
            max_wait_hours=VarMgr.get_int('amue_max_wait_hours', default=6, min_value=1),
            exponential_backoff=VarMgr.get('amue_polling_exponential_backoff', default='false').lower() == 'true',
            max_backoff_minutes=VarMgr.get_int('amue_polling_max_backoff_minutes', default=60, min_value=1)
        )

    # ── Délégation aux sous-composants ───────────────────────────────────────

    def _validate_finish_timestamp(self, finish_value: str) -> bool:
        """Délègue la validation du timestamp à FinishTimestampValidator."""
        return self._ts_validator.validate(finish_value)

    def _should_skip_import(self, current_finish: str) -> bool:
        """Décide si l'import doit être ignoré.

        Vérifie d'abord force_import localement (patchable dans les tests),
        puis délègue à FinishTimestampValidator pour la logique timestamp.
        """
        force_import = VarMgr.get('amue_force_import', default='false').lower() == 'true'
        if force_import:
            logger.info("[POLLING] Force import activé — skip désactivé")
            return False
        return self._ts_validator.should_skip(current_finish)

    def _calculate_max_attempts(self) -> int:
        """Délègue le calcul du nombre de tentatives à PollingStrategyCalculator."""
        return self._strategy.max_attempts()

    def _calculate_wait_time(self, attempt: int) -> float:
        """Délègue le calcul du temps d'attente à PollingStrategyCalculator."""
        return self._strategy.wait_time(attempt)

    # ── Boucle principale ────────────────────────────────────────────────────

    def wait_for_ready(self) -> Dict:
        """
        Attend que l'API soit prête (code 200 ET finish renseigné).

        Returns:
            Dictionnaire avec résultat du polling

        Raises:
            AirflowException: Si timeout atteint ou erreur critique
        """
        self.start_time = datetime.now()

        logger.info("[POLLING] Démarrage du service de polling")
        self._log_config()

        max_attempts = self._calculate_max_attempts()
        attempt = 0
        last_status_code = None
        last_finish_value = None

        while attempt < max_attempts:
            attempt += 1
            self._log_attempt(attempt, max_attempts)

            try:
                status_result = self.status_checker.fetch_full_status()

                status_code = status_result['http_status']
                finish_value = status_result.get('finish')
                last_status_code = status_code
                last_finish_value = finish_value

                logger.info(f"[POLLING] Code HTTP reçu: {status_code}")

                if status_code == 200:
                    logger.info(f"[POLLING] Variable 'finish' : {finish_value}")

                    if finish_value:
                        self._cached_tables_status = status_result.get('tables_status', {})
                        raw = status_result.get('raw_response') or {}
                        self._cached_report_start = raw.get('start', '')

                        if self._should_skip_import(finish_value):
                            logger.info("[POLLING] Timestamp inchangé - on continue le polling...")
                        else:
                            logger.info("[POLLING] API prête (finish renseigné, nouvelles données)")
                            return self._build_success_result(attempt, finish_value)
                    else:
                        logger.info("[POLLING] Traitement en cours côté AMUE (finish non renseigné)")

                elif self._is_critical_error(status_code):
                    raise AirflowException(
                        f"Code HTTP critique {status_code}. Arrêt du polling."
                    )

                elif self._is_server_error(status_code):
                    logger.warning(f"[POLLING] Erreur serveur {status_code}. Retry en cours...")

            except AirflowException:
                raise
            except Exception as e:
                logger.warning(f"[WARN] Erreur lors du polling: {str(e)}")
                last_status_code = None

            if attempt < max_attempts:
                wait_minutes = self._calculate_wait_time(attempt)
                self._wait_with_progress(wait_minutes)

        return self._build_timeout_result(attempt, last_status_code, last_finish_value)

    # ── Méthodes utilitaires ─────────────────────────────────────────────────

    def _log_config(self) -> None:
        logger.info("[POLLING] Configuration:")
        logger.info(f"  - Intervalle: {self.config.interval_minutes} minutes")
        logger.info(f"[POLLING] Max wait: {self.config.max_wait_hours} heures")
        if self.config.exponential_backoff:
            logger.info(f"  - Backoff exponentiel activé (max: {self.config.max_backoff_minutes}min)")
        else:
            logger.info("  - Intervalle fixe")

    def _wait_with_progress(self, wait_minutes: float) -> None:
        logger.info(f"[POLLING] Attente de {wait_minutes:.1f} minutes...")
        if wait_minutes > 5:
            intervals = 4
            interval_seconds = (wait_minutes * 60) / intervals
            for i in range(intervals):
                time.sleep(interval_seconds)
                progress = ((i + 1) / intervals) * 100
                elapsed = self._get_elapsed_time()
                logger.info(f"[POLLING] Progression: {progress:.0f}% (écoulé: {elapsed})")
        else:
            time.sleep(wait_minutes * 60)

    def _log_attempt(self, attempt: int, max_attempts: int) -> None:
        elapsed = self._get_elapsed_time()
        logger.info("[POLLING] ======================================")
        logger.info(f"[POLLING] Tentative {attempt}/{max_attempts}")
        logger.info(f"[POLLING] Temps écoulé: {elapsed}")
        logger.info("[POLLING] ======================================")

    def _get_elapsed_time(self) -> str:
        if not self.start_time:
            return "0m 0s"
        elapsed = datetime.now() - self.start_time
        total_seconds = int(elapsed.total_seconds())
        return f"{total_seconds // 60}m {total_seconds % 60}s"

    def _is_critical_error(self, status_code: int) -> bool:
        return 400 <= status_code < 500 and status_code != 429

    def _is_server_error(self, status_code: int) -> bool:
        return 500 <= status_code < 600

    def _build_success_result(self, attempt: int, finish_value: str = None) -> Dict:
        elapsed = datetime.now() - self.start_time
        wait_minutes = elapsed.total_seconds() / 60

        result = PollingResult(
            ready=True, attempts=attempt,
            total_wait_minutes=wait_minutes, status='success', last_status_code=200
        )

        logger.info(f"[POLLING] API prête après {attempt} tentative(s)")
        logger.info(f"[POLLING] Temps total: {wait_minutes:.1f} minutes")
        if finish_value:
            logger.info(f"[POLLING] Finish: {finish_value}")

        result_dict = self._result_to_dict(result)
        result_dict['finish'] = finish_value
        result_dict['report_start'] = self._cached_report_start or ''
        result_dict['start_time'] = self.start_time.isoformat()
        result_dict['tables_status'] = self._cached_tables_status or {}
        return result_dict

    def _build_timeout_result(
        self, attempt: int,
        last_status_code: Optional[int],
        last_finish_value: Optional[str] = None
    ) -> Dict:
        error_msg = (
            f"Timeout: API pas prête après {self.config.max_wait_hours}h "
            f"({attempt} tentatives, dernier code: {last_status_code}, "
            f"finish: {last_finish_value or 'non renseigné'})"
        )
        logger.error(f"[ERROR] {error_msg}")
        raise AirflowException(error_msg)

    def _result_to_dict(self, result: PollingResult) -> Dict:
        return {
            'ready': result.ready,
            'attempts': result.attempts,
            'total_wait_minutes': result.total_wait_minutes,
            'status': result.status,
            'last_status_code': result.last_status_code,
            'error': result.error
        }
