"""
Layer: application

Service de retry intelligent avec stratégies différenciées par type d'erreur.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module gère les tentatives de retry lors des appels API avec des stratégies
adaptées au TYPE d'erreur rencontré. Contrairement à un retry "bête" qui
réessaie toujours de la même façon, ce service adapte son comportement.

================================================================================
PHILOSOPHIE : RETRY INTELLIGENT
================================================================================

Toutes les erreurs ne méritent pas le même traitement :

┌────────────────┬──────────────┬────────────────────────────────────────────┐
│ Type d'erreur  │ Stratégie    │ Justification                              │
├────────────────┼──────────────┼────────────────────────────────────────────┤
│ 4xx Client     │ PAS de retry │ Erreur de paramètres → inutile de réessayer│
│ 429 Rate Limit │ Retry rapide │ Limite temporaire → réessayer vite         │
│ 5xx Serveur    │ Backoff long │ Serveur en panne → laisser le temps        │
│ Timeout        │ Retry court  │ Lenteur réseau → quelques tentatives       │
│ Connexion      │ Retry modéré │ Problème réseau → tenter quelques fois     │
└────────────────┴──────────────┴────────────────────────────────────────────┘

================================================================================
STRATÉGIES DÉTAILLÉES
================================================================================

ERREURS CLIENT (4xx sauf 429) :
    - max_retries: 0 (aucun retry)
    - Raison : Une erreur 400/401/403/404 indique un problème de requête,
               réessayer avec les mêmes paramètres n'a pas de sens

RATE LIMIT (429) :
    - max_retries: 5
    - base_delay: 2s → 4s → 8s → 16s → 30s (max)
    - jitter: oui (évite le "thundering herd")
    - Raison : Le serveur demande d'attendre, on attend puis on réessaie

ERREURS SERVEUR (5xx) :
    - max_retries: 3
    - base_delay: 5s → 10s → 20s → ... → 300s (max)
    - jitter: oui
    - Raison : Le serveur a un problème, on lui laisse le temps de récupérer

TIMEOUT :
    - max_retries: 2
    - base_delay: 3s (fixe)
    - Raison : Le réseau est lent mais pas cassé, quelques tentatives suffisent

ERREURS CONNEXION :
    - max_retries: 3
    - base_delay: 5s → 10s → 20s → ... → 60s (max)
    - jitter: oui
    - Raison : Problème réseau temporaire, on attend et on réessaie

================================================================================
CONCEPT : JITTER (BRUIT ALÉATOIRE)
================================================================================

Le jitter ajoute ±20% de variation aléatoire aux délais pour éviter le
problème du "thundering herd" : si 100 workers échouent en même temps et
réessaient exactement après 5s, ils vont tous frapper le serveur ensemble.

Avec jitter : délais entre 4s et 6s → les requêtes sont étalées.

================================================================================
USAGE
================================================================================

    # Via singleton (recommandé)
    from common.application.retry_service import get_retry_service

    service = get_retry_service()
    result = service.execute_with_retry(lambda: api.call())

    if result.success:
        print(result.result)
    else:
        print(f"Échec: {result.error}")
        print(f"Catégorie: {result.error_category}")
        print(f"Tentatives: {result.attempts}")

================================================================================
"""
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, TypeVar, Optional, Dict, Any

import requests

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ErrorCategory(Enum):
    """Catégories d'erreurs pour le retry intelligent"""
    CLIENT_ERROR = "client_error"       # 4xx (sauf 429) - pas de retry
    RATE_LIMITED = "rate_limited"       # 429 - retry agressif
    SERVER_ERROR = "server_error"       # 5xx - backoff exponentiel
    TIMEOUT = "timeout"                 # Timeout - retry court
    CONNECTION = "connection"           # Erreur réseau - retry modéré
    UNKNOWN = "unknown"                 # Autres - retry standard


@dataclass
class RetryStrategy:
    """Configuration d'une stratégie de retry"""
    max_retries: int
    base_delay: float           # Délai de base en secondes
    max_delay: float            # Délai maximum en secondes
    exponential: bool           # Utiliser le backoff exponentiel
    jitter: bool                # Ajouter du jitter aléatoire (±20%)

    def calculate_delay(self, attempt: int) -> float:
        """
        Calcule le délai pour une tentative donnée

        Args:
            attempt: Numéro de tentative (0-indexed)

        Returns:
            Délai en secondes
        """
        if self.exponential:
            delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        else:
            delay = min(self.base_delay, self.max_delay)

        if self.jitter:
            # Ajoute ±20% de jitter pour éviter le "thundering herd"
            jitter_factor = 1 + random.uniform(-0.2, 0.2)
            delay *= jitter_factor

        return delay


@dataclass
class RetryConfig:
    """Configuration globale du service de retry"""
    strategies: Dict[ErrorCategory, RetryStrategy] = field(default_factory=dict)

    def __post_init__(self):
        """Initialise les stratégies par défaut si non fournies"""
        defaults = {
            # 4xx (sauf 429) : PAS de retry
            ErrorCategory.CLIENT_ERROR: RetryStrategy(
                max_retries=0,
                base_delay=0,
                max_delay=0,
                exponential=False,
                jitter=False
            ),
            # 429 Rate Limit : Retry agressif avec délai court
            ErrorCategory.RATE_LIMITED: RetryStrategy(
                max_retries=5,
                base_delay=2.0,         # Démarre à 2s
                max_delay=30.0,         # Max 30s
                exponential=True,
                jitter=True             # Jitter pour éviter synchronisation
            ),
            # 5xx Serveur : Backoff exponentiel standard
            ErrorCategory.SERVER_ERROR: RetryStrategy(
                max_retries=3,
                base_delay=5.0,         # Démarre à 5s
                max_delay=300.0,        # Max 5 minutes
                exponential=True,
                jitter=True
            ),
            # Timeout : Retry court
            ErrorCategory.TIMEOUT: RetryStrategy(
                max_retries=2,
                base_delay=3.0,         # 3s entre tentatives
                max_delay=10.0,         # Max 10s
                exponential=False,      # Délai fixe
                jitter=False
            ),
            # Erreur connexion : Retry modéré
            ErrorCategory.CONNECTION: RetryStrategy(
                max_retries=3,
                base_delay=5.0,
                max_delay=60.0,
                exponential=True,
                jitter=True
            ),
            # Inconnu : Retry standard
            ErrorCategory.UNKNOWN: RetryStrategy(
                max_retries=3,
                base_delay=5.0,
                max_delay=60.0,
                exponential=True,
                jitter=True
            ),
        }

        for category, strategy in defaults.items():
            if category not in self.strategies:
                self.strategies[category] = strategy

    def get_strategy(self, category: ErrorCategory) -> RetryStrategy:
        """Retourne la stratégie pour une catégorie d'erreur"""
        return self.strategies.get(category, self.strategies[ErrorCategory.UNKNOWN])


@dataclass
class RetryResult:
    """Résultat d'une opération avec retry"""
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    attempts: int = 0
    total_delay: float = 0.0
    error_category: Optional[ErrorCategory] = None

    def __bool__(self) -> bool:
        return self.success


class RetryService:
    """
    Service de retry intelligent avec stratégies par type d'erreur

    Example:
        >>> service = RetryService()
        >>> result = service.execute_with_retry(lambda: api.call())
        >>> if result.success:
        ...     print(result.result)
        ... else:
        ...     print(f"Échec après {result.attempts} tentatives: {result.error}")
    """

    def __init__(self, config: Optional[RetryConfig] = None):
        """
        Initialise le service de retry

        Args:
            config: Configuration personnalisée (optionnel)
        """
        self.config = config or RetryConfig()

    @staticmethod
    def categorize_error(error: Exception) -> ErrorCategory:
        """
        Catégorise une exception pour déterminer la stratégie de retry

        Args:
            error: L'exception à catégoriser

        Returns:
            La catégorie d'erreur appropriée
        """
        # Timeout
        if isinstance(error, requests.exceptions.Timeout):
            return ErrorCategory.TIMEOUT

        # Erreurs de connexion
        if isinstance(error, requests.exceptions.ConnectionError):
            return ErrorCategory.CONNECTION

        # Erreurs HTTP
        if isinstance(error, requests.exceptions.HTTPError):
            if error.response is not None:
                status_code = error.response.status_code

                # Rate limit (429)
                if status_code == 429:
                    return ErrorCategory.RATE_LIMITED

                # Erreurs client (4xx) - pas de retry
                if 400 <= status_code < 500:
                    return ErrorCategory.CLIENT_ERROR

                # Erreurs serveur (5xx)
                if 500 <= status_code < 600:
                    return ErrorCategory.SERVER_ERROR

        # Erreurs réseau génériques
        if isinstance(error, requests.exceptions.RequestException):
            return ErrorCategory.CONNECTION

        return ErrorCategory.UNKNOWN

    @staticmethod
    def categorize_status_code(status_code: int) -> ErrorCategory:
        """
        Catégorise un code HTTP pour déterminer la stratégie de retry

        Args:
            status_code: Code HTTP

        Returns:
            La catégorie d'erreur appropriée
        """
        if status_code == 429:
            return ErrorCategory.RATE_LIMITED
        if 400 <= status_code < 500:
            return ErrorCategory.CLIENT_ERROR
        if 500 <= status_code < 600:
            return ErrorCategory.SERVER_ERROR
        return ErrorCategory.UNKNOWN

    def should_retry(self, error: Exception, attempt: int) -> tuple[bool, float]:
        """
        Détermine si on doit retry et avec quel délai

        Args:
            error: L'exception survenue
            attempt: Numéro de tentative actuel (0-indexed)

        Returns:
            Tuple (should_retry, delay_seconds)
        """
        category = self.categorize_error(error)
        strategy = self.config.get_strategy(category)

        if attempt >= strategy.max_retries:
            return False, 0.0

        delay = strategy.calculate_delay(attempt)
        return True, delay

    def execute_with_retry(
        self,
        operation: Callable[[], T],
        on_retry: Optional[Callable[[int, Exception, float], None]] = None
    ) -> RetryResult:
        """
        Exécute une opération avec retry intelligent

        Args:
            operation: Fonction à exécuter
            on_retry: Callback appelé avant chaque retry (attempt, error, delay)

        Returns:
            RetryResult avec le résultat ou l'erreur
        """
        attempt = 0
        total_delay = 0.0
        last_error: Optional[Exception] = None
        last_category: Optional[ErrorCategory] = None

        while True:
            try:
                result = operation()
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempt + 1,
                    total_delay=total_delay
                )

            except Exception as e:
                last_error = e
                last_category = self.categorize_error(e)

                should_retry, delay = self.should_retry(e, attempt)

                if not should_retry:
                    logger.warning(
                        f"[RETRY] Abandon après {attempt + 1} tentative(s) - "
                        f"Catégorie: {last_category.value}, Erreur: {e}"
                    )
                    return RetryResult(
                        success=False,
                        error=last_error,
                        attempts=attempt + 1,
                        total_delay=total_delay,
                        error_category=last_category
                    )

                logger.info(
                    f"[RETRY] Tentative {attempt + 1} échouée - "
                    f"Catégorie: {last_category.value}, "
                    f"Retry dans {delay:.1f}s"
                )

                if on_retry:
                    on_retry(attempt, e, delay)

                time.sleep(delay)
                total_delay += delay
                attempt += 1

    def execute_with_retry_http(
        self,
        operation: Callable[[], requests.Response],
        on_retry: Optional[Callable[[int, Exception, float], None]] = None
    ) -> RetryResult:
        """
        Exécute une requête HTTP avec retry intelligent

        Vérifie automatiquement le status code de la réponse
        et applique la stratégie appropriée.

        Args:
            operation: Fonction retournant une Response
            on_retry: Callback appelé avant chaque retry

        Returns:
            RetryResult avec la Response ou l'erreur
        """
        def wrapped_operation():
            response = operation()
            response.raise_for_status()
            return response

        return self.execute_with_retry(wrapped_operation, on_retry)

    def get_retry_info(self, error: Exception) -> Dict[str, Any]:
        """
        Retourne les informations de retry pour une erreur

        Args:
            error: L'exception à analyser

        Returns:
            Dict avec catégorie, stratégie et recommandations
        """
        category = self.categorize_error(error)
        strategy = self.config.get_strategy(category)

        recommendations = {
            ErrorCategory.CLIENT_ERROR: "Vérifier les paramètres de la requête. Pas de retry automatique.",
            ErrorCategory.RATE_LIMITED: "Rate limit atteint. Retry automatique avec backoff.",
            ErrorCategory.SERVER_ERROR: "Erreur serveur. Retry automatique avec backoff exponentiel.",
            ErrorCategory.TIMEOUT: "Timeout réseau. Retry automatique avec délai court.",
            ErrorCategory.CONNECTION: "Erreur de connexion. Vérifier le réseau. Retry automatique.",
            ErrorCategory.UNKNOWN: "Erreur inconnue. Retry automatique avec stratégie standard.",
        }

        return {
            'category': category.value,
            'max_retries': strategy.max_retries,
            'base_delay': strategy.base_delay,
            'max_delay': strategy.max_delay,
            'exponential': strategy.exponential,
            'jitter': strategy.jitter,
            'recommendation': recommendations.get(category, "Erreur non catégorisée")
        }


# Instance singleton pour utilisation globale (thread-safe)
_default_service: Optional[RetryService] = None
_service_lock = threading.Lock()


def get_retry_service() -> RetryService:
    """
    Retourne l'instance singleton du service de retry (thread-safe)

    Returns:
        Instance de RetryService
    """
    global _default_service
    if _default_service is None:
        with _service_lock:
            # Double-check locking pattern
            if _default_service is None:
                _default_service = RetryService()
    return _default_service


def reset_retry_service() -> None:
    """Réinitialise le service singleton (utile pour les tests, thread-safe)"""
    global _default_service
    with _service_lock:
        _default_service = None
