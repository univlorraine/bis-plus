"""
Layer: domain

Calculateur de stratégie de polling pour l'API AMUE.

Détermine le nombre maximum de tentatives et le temps d'attente
entre chaque tentative selon la stratégie configurée.
"""
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amue.application.polling_service import PollingConfig

logger = logging.getLogger(__name__)


class PollingStrategyCalculator:
    """
    Calcule les paramètres de timing du polling.

    Supporte deux stratégies :
        - Intervalle fixe : toutes les N minutes
        - Backoff exponentiel : 2^(tentative-1) × intervalle, max configuré

    Example:
        >>> from amue.application.polling_service import PollingConfig
        >>> config = PollingConfig(interval_minutes=10, max_wait_hours=6)
        >>> calc = PollingStrategyCalculator(config)
        >>> calc.max_attempts()  # 36
        >>> calc.wait_time(1)    # 10.0
        >>> calc.wait_time(2)    # 10.0 (fixe) ou 20.0 (backoff)
    """

    def __init__(self, config: 'PollingConfig'):
        """
        Args:
            config: Configuration du service de polling
        """
        self.config = config

    def max_attempts(self) -> int:
        """
        Calcule le nombre maximum de tentatives dans la fenêtre de temps.

        Returns:
            Nombre de tentatives possibles (minimum 1)
        """
        total_minutes = self.config.max_wait_hours * 60
        return max(1, total_minutes // self.config.interval_minutes)

    def wait_time(self, attempt: int) -> float:
        """
        Calcule le temps d'attente selon la stratégie configurée.

        Args:
            attempt: Numéro de la tentative actuelle (1-indexed)

        Returns:
            Temps d'attente en minutes
        """
        if not self.config.exponential_backoff:
            return self.config.interval_minutes

        # Backoff exponentiel : 2^(attempt-1) * interval, plafonné à max_backoff
        wait = self.config.interval_minutes * (2 ** (attempt - 1))
        return min(wait, self.config.max_backoff_minutes)
