"""
Layer: application

État blue/green et gestionnaire de persistance en base de données.

Ce module contient :
    - BlueGreenStateManager : lecture/écriture de l'état dans splus_admin.amue_state

`BlueGreenState` est défini dans `state_types.py` (pour éviter le cycle
d'imports avec `admin_state_manager`) et réexporté ici pour compatibilité.
"""
import logging

from common.application.admin_state_manager import AdminStateManager
from common.domain.state_types import BlueGreenState

logger = logging.getLogger(__name__)

__all__ = ['BlueGreenState', 'BlueGreenStateManager']


class BlueGreenStateManager:
    """
    Chargement et sauvegarde de l'état blue/green depuis splus_admin.amue_state.

    Toutes les opérations vont directement en base (pas de cache interne)
    pour garantir la fraîcheur des données. Le cache est géré par BlueGreenManager.
    """

    def load_state(self) -> BlueGreenState:
        """
        Charge l'état depuis la base de données.

        Returns:
            BlueGreenState chargé ou état par défaut si erreur
        """
        try:
            state = AdminStateManager().get_bluegreen_state()
            return state if state is not None else BlueGreenState()
        except Exception as e:
            logger.warning(f"[BLUEGREEN] Erreur chargement état: {e}, utilisation état par défaut")
            return BlueGreenState()

    def save_state(self, state: BlueGreenState) -> bool:
        """
        Sauvegarde l'état dans la base de données.

        Args:
            state: État à sauvegarder

        Returns:
            True si sauvegarde réussie
        """
        try:
            success = AdminStateManager().save_bluegreen_state(state)
            if success:
                logger.info(f"[BLUEGREEN] État sauvegardé: {state.to_dict()}")
            return success
        except Exception as e:
            logger.error(f"[BLUEGREEN] Erreur sauvegarde état: {e}")
            return False

    def mark_switch_completed(self, new_active: str) -> bool:
        """
        Met à jour last_switch_timestamp et active_schema en BDD après switch.

        Args:
            new_active: Schéma court qui est maintenant actif (ex: 'green')

        Returns:
            True si mise à jour réussie
        """
        logger.info("[BLUEGREEN] Switch effectué")
        return AdminStateManager().mark_switch_completed(new_active)

    def mark_sync_completed(self) -> bool:
        """
        Met à jour last_sync_timestamp en BDD.

        Returns:
            True si mise à jour réussie
        """
        logger.info("[BLUEGREEN] Sync terminée")
        return AdminStateManager().mark_sync_completed()
