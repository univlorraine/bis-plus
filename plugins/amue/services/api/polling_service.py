"""
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

Les deux conditions doivent être satisfaites pour considérer l'API comme "prête".

================================================================================
STRATÉGIES DE POLLING
================================================================================

INTERVALLE FIXE (par défaut) :
    Vérifie l'API toutes les N minutes (configurable)
    Exemple : toutes les 10 minutes pendant 6 heures max

BACKOFF EXPONENTIEL (optionnel) :
    Augmente progressivement l'intervalle entre les vérifications
    Utile pour réduire la charge sur l'API si elle est lente à démarrer
    Exemple : 10min → 20min → 40min → 60min (max)

================================================================================
GESTION DES ERREURS
================================================================================

ERREURS CRITIQUES (arrêt immédiat) :
    - 4xx (sauf 429) : Erreur de configuration ou d'authentification
    - Le polling s'arrête et le DAG échoue

ERREURS TRANSITOIRES (retry) :
    - 5xx : Erreur serveur AMUE temporaire
    - 429 : Rate limit (trop de requêtes)
    - Timeout : Problème réseau temporaire
    - Le polling continue jusqu'au timeout global

================================================================================
CONFIGURATION
================================================================================

Variables Airflow :
    - amue_polling_interval_minutes : Intervalle entre vérifications (défaut: 10)
    - amue_max_wait_hours : Durée max d'attente (défaut: 6)
    - amue_polling_exponential_backoff : Active le backoff (défaut: false)
    - amue_polling_max_backoff_minutes : Intervalle max en backoff (défaut: 60)

================================================================================
MÉTRIQUES COLLECTÉES
================================================================================

Le service collecte des métriques pour le rapport final :
    - Nombre de tentatives
    - Temps total d'attente
    - Valeur de 'finish' (horodatage de fin AMUE)
    - Codes HTTP reçus

================================================================================
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


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
    - Cache du tables_status pour éviter un appel API supplémentaire
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
        self._cached_tables_status = None  # Cache pour éviter appel API supplémentaire

    def _load_default_config(self) -> PollingConfig:
        """Charge la configuration depuis les variables Airflow"""
        return PollingConfig(
            interval_minutes=int(VarMgr.get('amue_polling_interval_minutes', default='10')),
            max_wait_hours=int(VarMgr.get('amue_max_wait_hours', default='6')),
            exponential_backoff=VarMgr.get('amue_polling_exponential_backoff', default='False').lower() == 'true',
            max_backoff_minutes=int(VarMgr.get('amue_polling_max_backoff_minutes', default='60'))
        )

    def _validate_finish_timestamp(self, finish_value: str) -> bool:
        """
        Valide le format du timestamp finish retourné par l'API.

        Args:
            finish_value: Valeur du finish à valider

        Returns:
            True si le format est valide
        """
        if not finish_value or not finish_value.strip():
            logger.warning("[POLLING] Finish timestamp vide ou invalide")
            return False

        # Vérifie que ce n'est pas une valeur par défaut ou placeholder
        invalid_values = ['', 'null', 'none', 'undefined', '0', '00000000']
        if finish_value.lower().strip() in invalid_values:
            logger.warning(f"[POLLING] Finish timestamp invalide: '{finish_value}'")
            return False

        # Log le format pour traçabilité
        logger.info(f"[POLLING] Finish timestamp valide: {finish_value}")
        return True

    def _should_skip_import(self, current_finish: str) -> bool:
        """
        Vérifie si l'import doit être ignoré car le timestamp finish est inchangé.

        Compare le timestamp finish actuel avec celui du dernier import réussi.
        Si identique, cela signifie qu'aucune nouvelle donnée n'est disponible.

        Comportement:
        - Première exécution (pas de timestamp stocké): import exécuté
        - Force import activé: import toujours exécuté
        - Même timestamp: import ignoré
        - Nouveau timestamp: import exécuté

        Args:
            current_finish: Timestamp finish retourné par l'API

        Returns:
            True si l'import doit être ignoré
        """
        # Option pour forcer l'import (utile pour debug ou réimport manuel)
        force_import = VarMgr.get('amue_force_import', default='false').lower() == 'true'
        if force_import:
            logger.info("[POLLING] Force import activé (amue_force_import=true)")
            logger.info("[POLLING] Import sera exécuté même si timestamp inchangé")
            return False

        # Valide le timestamp courant
        if not self._validate_finish_timestamp(current_finish):
            logger.warning("[POLLING] Finish invalide - import exécuté par précaution")
            return False

        # Récupère le timestamp précédent
        stored_finish = VarMgr.get('amue_last_finish_timestamp', default='')

        # Première exécution - pas de timestamp précédent
        if not stored_finish or not stored_finish.strip():
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] PREMIÈRE EXÉCUTION DÉTECTÉE")
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] Aucun timestamp précédent enregistré")
            logger.info(f"[POLLING] Timestamp actuel de l'API: {current_finish}")
            logger.info("[POLLING] L'import sera exécuté et ce timestamp sera sauvegardé")
            logger.info("[POLLING] Les prochaines exécutions compareront avec cette valeur")
            return False

        # Comparaison des timestamps
        if stored_finish == current_finish:
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] TIMESTAMP INCHANGÉ")
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info(f"[POLLING] Timestamp stocké:  {stored_finish}")
            logger.info(f"[POLLING] Timestamp actuel:  {current_finish}")
            logger.info("[POLLING] Pas de nouvelles données disponibles")
            logger.info("[POLLING] Pour forcer l'import: amue_force_import=true")
            return True

        # Nouveau timestamp détecté
        logger.info("[POLLING] ═══════════════════════════════════════════")
        logger.info("[POLLING] NOUVEAU TIMESTAMP DÉTECTÉ")
        logger.info("[POLLING] ═══════════════════════════════════════════")
        logger.info(f"[POLLING] Timestamp précédent: {stored_finish}")
        logger.info(f"[POLLING] Timestamp actuel:    {current_finish}")
        logger.info("[POLLING] De nouvelles données sont disponibles")
        return False

    def wait_for_ready(self) -> Dict:
        """
        Attend que l'API soit prête (code 200 ET finish renseigné)

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

            # Log de progression
            self._log_attempt(attempt, max_attempts)

            # Vérification du statut - UN SEUL appel API au lieu de 2
            try:
                status_result = self.status_checker.fetch_full_status()

                status_code = status_result['http_status']
                finish_value = status_result.get('finish')
                last_status_code = status_code
                last_finish_value = finish_value

                logger.info(f"[POLLING] Code HTTP reçu: {status_code}")

                # Si code 200, vérifier la variable 'finish'
                if status_code == 200:
                    logger.info(f"[POLLING] Variable 'finish' : {finish_value}")

                    if finish_value:
                        # Stocke les tables_status pour éviter l'appel supplémentaire
                        self._cached_tables_status = status_result.get('tables_status', {})

                        # Vérifie si le timestamp est nouveau
                        if self._should_skip_import(finish_value):
                            logger.info("[POLLING] Timestamp inchangé - on continue le polling...")
                            # On continue la boucle au lieu de skip
                        else:
                            logger.info("[POLLING] API prête (finish renseigné, nouvelles données)")
                            return self._build_success_result(attempt, finish_value)
                    else:
                        logger.info("[POLLING] Traitement en cours côté AMUE (finish non renseigné)")
                        logger.info("[POLLING] Attente de la fin du traitement...")

                # Codes d'erreur critiques (pas besoin de retry)
                elif self._is_critical_error(status_code):
                    raise AirflowException(
                        f"Code HTTP critique {status_code}. Arrêt du polling."
                    )

                # Erreurs serveur (5xx) - on continue mais on log l'erreur
                elif self._is_server_error(status_code):
                    logger.warning(f"[POLLING] Erreur serveur {status_code}. Retry en cours...")

            except AirflowException:
                raise
            except Exception as e:
                logger.warning(f"[WARN] Erreur lors du polling: {str(e)}")
                last_status_code = None

            # Attente avant prochaine tentative
            if attempt < max_attempts:
                wait_minutes = self._calculate_wait_time(attempt)
                self._wait_with_progress(wait_minutes)

        # Timeout atteint
        return self._build_timeout_result(attempt, last_status_code, last_finish_value)

    def _log_config(self) -> None:
        """Affiche la configuration du polling"""
        logger.info(f"[POLLING] Configuration:")
        logger.info(f"  - Intervalle: {self.config.interval_minutes} minutes")
        logger.info(f"[POLLING] Max wait: {self.config.max_wait_hours} heures")

        if self.config.exponential_backoff:
            logger.info(f"  - Backoff exponentiel activé (max: {self.config.max_backoff_minutes}min)")
        else:
            logger.info(f"  - Intervalle fixe")

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
        logger.info(f"[POLLING] Attente de {wait_minutes:.1f} minutes...")

        # Pour les attentes longues, affiche une progression
        if wait_minutes > 5:
            intervals = 4  # Affiche 4 points de progression
            interval_seconds = (wait_minutes * 60) / intervals

            for i in range(intervals):
                time.sleep(interval_seconds)
                progress = ((i + 1) / intervals) * 100
                elapsed = self._get_elapsed_time()
                logger.info(f"[POLLING] Progression: {progress:.0f}% (écoulé: {elapsed})")
        else:
            time.sleep(wait_minutes * 60)

    def _log_attempt(self, attempt: int, max_attempts: int) -> None:
        """Log une tentative de polling"""
        elapsed = self._get_elapsed_time()
        logger.info(f"[POLLING] ======================================")
        logger.info(f"[POLLING] Tentative {attempt}/{max_attempts}")
        logger.info(f"[POLLING] Temps écoulé: {elapsed}")
        logger.info(f"[POLLING] ======================================")

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

    def _is_server_error(self, status_code: int) -> bool:
        """
        Détermine si un code HTTP est une erreur serveur

        Args:
            status_code: Code HTTP reçu

        Returns:
            True si erreur serveur (5xx)
        """
        return 500 <= status_code < 600

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

        logger.info(f"[POLLING] API prête après {attempt} tentative(s)")
        logger.info(f"[POLLING] Temps total: {wait_minutes:.1f} minutes")
        if finish_value:
            logger.info(f"[POLLING] Finish: {finish_value}")

        result_dict = self._result_to_dict(result)
        result_dict['finish'] = finish_value
        result_dict['start_time'] = self.start_time.isoformat()
        # Inclut tables_status pour éviter appel API supplémentaire dans le DAG
        result_dict['tables_status'] = self._cached_tables_status or {}
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

        logger.error(f"[ERROR] {error_msg}")

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
