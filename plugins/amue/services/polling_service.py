"""
Service de polling intelligent pour l'API AMUE
Attend que l'API soit disponible avec retry exponentiel et timeout configurable
Vérifie à la fois le code HTTP 200 ET la variable 'finish' du JSON
"""
import time
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr


@dataclass
class PollingConfig:
    """Configuration du service de polling"""
    interval_minutes: int
    max_wait_hours: int
    exponential_backoff: bool = False
    max_backoff_minutes: int = 60


@dataclass
class PollingResult:
    """Résultat d'une opération de polling"""
    ready: bool
    attempts: int
    total_wait_minutes: float
    status: str
    last_status_code: Optional[int] = None
    error: Optional[str] = None


class AMUEPollingService:
    """
    Service de polling pour attendre la disponibilité de l'API AMUE

    Fonctionnalités :
    - Retry configurable avec intervalle fixe ou exponentiel
    - Timeout global pour éviter les attentes infinies
    - Logs détaillés de progression
    - Métriques d'exécution (tentatives, temps d'attente)
    """

    def __init__(self, status_checker, config: Optional[PollingConfig] = None):
        """
        Initialise le service de polling

        Args:
            status_checker: Instance de AMUEStatusChecker
            config: Configuration personnalisée (optionnelle)
        """
        self.status_checker = status_checker
        self.config = config or self._load_default_config()
        self.start_time = None

    def _load_default_config(self) -> PollingConfig:
        """Charge la configuration depuis les variables Airflow"""
        return PollingConfig(
            interval_minutes=int(VarMgr.get('amue_polling_interval_minutes', default='10')),
            max_wait_hours=int(VarMgr.get('amue_max_wait_hours', default='6')),
            exponential_backoff=VarMgr.get('amue_polling_exponential_backoff', default='False').lower() == 'true',
            max_backoff_minutes=int(VarMgr.get('amue_polling_max_backoff_minutes', default='60'))
        )

    def wait_for_ready(self) -> Dict:
        """
        Attend que l'API soit prête (code 200 ET finish renseigné)

        Returns:
            Dictionnaire avec résultat du polling

        Raises:
            AirflowException: Si timeout atteint ou erreur critique
        """
        self.start_time = datetime.now()

        print("[POLLING] Démarrage du service de polling")
        self._log_config()

        max_attempts = self._calculate_max_attempts()
        attempt = 0
        last_status_code = None
        last_finish_value = None

        while attempt < max_attempts:
            attempt += 1

            # Log de progression
            self._log_attempt(attempt, max_attempts)

            # Vérification du statut
            try:
                status_code = self.status_checker.check_status_code()
                last_status_code = status_code

                print(f"[POLLING] Code HTTP reçu: {status_code}")

                # Si code 200, vérifier la variable 'finish'
                if status_code == 200:
                    finish_value = self.status_checker.check_finish_status()
                    last_finish_value = finish_value

                    print(f"[POLLING] Variable 'finish' : {finish_value}")

                    if finish_value:
                        print("[POLLING] ✓ API prête (finish renseigné)")
                        return self._build_success_result(attempt, finish_value)
                    else:
                        print("[POLLING] ⏳ Traitement en cours côté AMUE (finish non renseigné)")
                        print("[POLLING] Attente de la fin du traitement...")

                # Codes d'erreur critiques (pas besoin de retry)
                elif self._is_critical_error(status_code):
                    raise AirflowException(
                        f"Code HTTP critique {status_code}. Arrêt du polling."
                    )

            except AirflowException:
                raise
            except Exception as e:
                print(f"[WARN] Erreur lors du polling: {str(e)}")
                last_status_code = None

            # Attente avant prochaine tentative
            if attempt < max_attempts:
                wait_minutes = self._calculate_wait_time(attempt)
                self._wait_with_progress(wait_minutes)

        # Timeout atteint
        return self._build_timeout_result(attempt, last_status_code, last_finish_value)

    def _log_config(self) -> None:
        """Affiche la configuration du polling"""
        print(f"[POLLING] Configuration:")
        print(f"  - Intervalle: {self.config.interval_minutes} minutes")
        print(f"[POLLING] Max wait: {self.config.max_wait_hours} heures")

        if self.config.exponential_backoff:
            print(f"  - Backoff exponentiel activé (max: {self.config.max_backoff_minutes}min)")
        else:
            print(f"  - Intervalle fixe")

    def _calculate_max_attempts(self) -> int:
        """
        Calcule le nombre maximum de tentatives

        Returns:
            Nombre de tentatives possibles dans la fenêtre de temps
        """
        total_minutes = self.config.max_wait_hours * 60
        return max(1, total_minutes // self.config.interval_minutes)

    def _calculate_wait_time(self, attempt: int) -> float:
        """
        Calcule le temps d'attente selon la stratégie configurée

        Args:
            attempt: Numéro de la tentative actuelle

        Returns:
            Temps d'attente en minutes
        """
        if not self.config.exponential_backoff:
            return self.config.interval_minutes

        # Backoff exponentiel: 2^(attempt-1) * interval
        wait = self.config.interval_minutes * (2 ** (attempt - 1))
        return min(wait, self.config.max_backoff_minutes)

    def _wait_with_progress(self, wait_minutes: float) -> None:
        """
        Attend avec affichage de progression

        Args:
            wait_minutes: Temps d'attente en minutes
        """
        print(f"[POLLING] Attente de {wait_minutes:.1f} minutes...")

        # Pour les attentes longues, affiche une progression
        if wait_minutes > 5:
            intervals = 4  # Affiche 4 points de progression
            interval_seconds = (wait_minutes * 60) / intervals

            for i in range(intervals):
                time.sleep(interval_seconds)
                progress = ((i + 1) / intervals) * 100
                elapsed = self._get_elapsed_time()
                print(f"[POLLING] Progression: {progress:.0f}% (écoulé: {elapsed})")
        else:
            time.sleep(wait_minutes * 60)

    def _log_attempt(self, attempt: int, max_attempts: int) -> None:
        """Log une tentative de polling"""
        elapsed = self._get_elapsed_time()
        print(f"[POLLING] ======================================")
        print(f"[POLLING] Tentative {attempt}/{max_attempts}")
        print(f"[POLLING] Temps écoulé: {elapsed}")
        print(f"[POLLING] ======================================")

    def _get_elapsed_time(self) -> str:
        """
        Retourne le temps écoulé depuis le début

        Returns:
            Chaîne formatée (ex: "15m 30s")
        """
        if not self.start_time:
            return "0m 0s"

        elapsed = datetime.now() - self.start_time
        total_seconds = int(elapsed.total_seconds())
        minutes = total_seconds // 60
        seconds = total_seconds % 60

        return f"{minutes}m {seconds}s"

    def _is_critical_error(self, status_code: int) -> bool:
        """
        Détermine si un code HTTP est une erreur critique

        Args:
            status_code: Code HTTP reçu

        Returns:
            True si erreur critique (pas besoin de retry)
        """
        # 4xx : Erreurs client (sauf 429 Too Many Requests qui mérite retry)
        if 400 <= status_code < 500 and status_code != 429:
            return True

        return False

    def _build_success_result(self, attempt: int, finish_value: str = None) -> Dict:
        """Construit le résultat en cas de succès"""
        elapsed = datetime.now() - self.start_time
        wait_minutes = elapsed.total_seconds() / 60

        result = PollingResult(
            ready=True,
            attempts=attempt,
            total_wait_minutes=wait_minutes,
            status='success',
            last_status_code=200
        )

        print(f"[POLLING] ✓ API prête après {attempt} tentative(s)")
        print(f"[POLLING] Temps total: {wait_minutes:.1f} minutes")
        if finish_value:
            print(f"[POLLING] Finish: {finish_value}")

        result_dict = self._result_to_dict(result)
        result_dict['finish'] = finish_value
        return result_dict

    def _build_timeout_result(
        self,
        attempt: int,
        last_status_code: Optional[int],
        last_finish_value: Optional[str] = None
    ) -> Dict:
        """
        Construit le résultat en cas de timeout

        Raises:
            AirflowException: Toujours (timeout = échec)
        """
        elapsed = datetime.now() - self.start_time
        wait_minutes = elapsed.total_seconds() / 60

        error_msg = (
            f"Timeout: API pas prête après {self.config.max_wait_hours}h "
            f"({attempt} tentatives, dernier code: {last_status_code}, "
            f"finish: {last_finish_value or 'non renseigné'})"
        )

        print(f"[ERROR] {error_msg}")

        result = PollingResult(
            ready=False,
            attempts=attempt,
            total_wait_minutes=wait_minutes,
            status='timeout',
            last_status_code=last_status_code,
            error=error_msg
        )

        raise AirflowException(error_msg)

    def _result_to_dict(self, result: PollingResult) -> Dict:
        """Convertit un résultat en dictionnaire"""
        return {
            'ready': result.ready,
            'attempts': result.attempts,
            'total_wait_minutes': result.total_wait_minutes,
            'status': result.status,
            'last_status_code': result.last_status_code,
            'error': result.error
        }