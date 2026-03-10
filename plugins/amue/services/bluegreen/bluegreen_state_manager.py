"""
État blue/green et gestionnaire de persistance en base de données.

Ce module contient :
    - BlueGreenState : dataclass immutable représentant l'état du déploiement
    - BlueGreenStateManager : lecture/écriture de l'état dans splus_admin.amue_state
"""
import logging
from dataclasses import dataclass
from typing import Dict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlueGreenState:
    """État du déploiement blue/green (verrou d'import + audit)."""

    last_import_schema: str = ""
    last_switch_timestamp: str = ""
    last_sync_timestamp: str = ""
    import_in_progress: bool = False
    import_started_at: str = ""        # Timestamp ISO du début de l'import
    import_correlation_id: str = ""    # ID de corrélation de l'import en cours

    def to_dict(self) -> Dict:
        """Convertit l'état en dictionnaire."""
        return {
            "last_import_schema": self.last_import_schema,
            "last_switch_timestamp": self.last_switch_timestamp,
            "last_sync_timestamp": self.last_sync_timestamp,
            "import_in_progress": self.import_in_progress,
            "import_started_at": self.import_started_at,
            "import_correlation_id": self.import_correlation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'BlueGreenState':
        """Crée un état depuis un dictionnaire (ignore les champs inconnus)."""
        return cls(
            last_import_schema=data.get("last_import_schema", ""),
            last_switch_timestamp=data.get("last_switch_timestamp", ""),
            last_sync_timestamp=data.get("last_sync_timestamp", ""),
            import_in_progress=data.get("import_in_progress", False),
            import_started_at=data.get("import_started_at", ""),
            import_correlation_id=data.get("import_correlation_id", ""),
        )


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
        from amue.services.admin_state_manager import AdminStateManager
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
        from amue.services.admin_state_manager import AdminStateManager
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
        from amue.services.admin_state_manager import AdminStateManager
        logger.info("[BLUEGREEN] Switch effectué")
        return AdminStateManager().mark_switch_completed(new_active)

    def mark_sync_completed(self) -> bool:
        """
        Met à jour last_sync_timestamp en BDD.

        Returns:
            True si mise à jour réussie
        """
        from amue.services.admin_state_manager import AdminStateManager
        logger.info("[BLUEGREEN] Sync terminée")
        return AdminStateManager().mark_sync_completed()
