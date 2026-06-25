"""
Layer: application

Gestion du verrou d'import exclusif pour le mode blue/green.

Le verrou est implémenté via un UPDATE ... WHERE NOT import_in_progress
RETURNING id dans splus_admin.amue_state, garantissant l'atomicité
même en environnement multi-processus/multi-worker.
"""
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from common.domain.exceptions import ConcurrentImportError
from common.application.admin_state_manager import AdminStateManager

if TYPE_CHECKING:
    from common.domain.state_types import BlueGreenState

logger = logging.getLogger(__name__)

#: Durée maximale d'un import (verrou abandonné au-delà).
BLUEGREEN_LOCK_TIMEOUT_MINUTES: int = 120


class BlueGreenLockManager:
    """
    Gestion du verrou d'import atomique pour le mode blue/green.

    Toutes les opérations passent directement par AdminStateManager
    sans cache interne. Le cache est géré par BlueGreenManager.

    Example:
        >>> lock_mgr = BlueGreenLockManager()
        >>> lock_mgr.acquire_lock('run-abc123', current_state)
        >>> lock_mgr.release_lock('green', current_state)
    """

    def acquire_lock(self, correlation_id: str, state: 'BlueGreenState') -> bool:
        """
        Acquiert le verrou d'import de manière atomique.

        Utilise UPDATE ... WHERE NOT import_in_progress RETURNING id pour
        éviter toute race condition. Gère les verrous abandonnés (stale).

        Args:
            correlation_id: ID de corrélation pour tracer l'import
            state: État actuel (pour détecter les verrous stale)

        Returns:
            True si le verrou a été acquis

        Raises:
            ConcurrentImportError: Si un autre import est en cours
        """
        mgr = AdminStateManager()
        started_at = datetime.now().isoformat()

        # Tentative atomique d'acquisition
        if mgr.try_acquire_import_lock(started_at, correlation_id):
            logger.info(
                f"[BLUEGREEN] Verrou acquis pour import "
                f"(correlation_id: {correlation_id or 'N/A'})"
            )
            return True

        # Échec → vérifier si verrou stale
        if self.is_stale(state):
            logger.warning(
                f"[BLUEGREEN] Verrou abandonné détecté (démarré: {state.import_started_at}). "
                f"Libération automatique."
            )
            mgr.force_release_lock()
            if mgr.try_acquire_import_lock(started_at, correlation_id):
                logger.info(
                    f"[BLUEGREEN] Verrou acquis après libération stale "
                    f"(correlation_id: {correlation_id or 'N/A'})"
                )
                return True

        raise ConcurrentImportError(
            f"Un import est déjà en cours depuis {state.import_started_at}",
            import_started_at=state.import_started_at,
            context={"correlation_id": state.import_correlation_id}
        )

    def release_lock(self, active_schema: str, state: 'BlueGreenState') -> bool:
        """
        Libère le verrou d'import.

        Args:
            active_schema: Schéma court (ex: 'green') à enregistrer comme dernier importé
            state: État actuel pour les logs

        Returns:
            True si le verrou a été libéré
        """
        old_started_at = state.import_started_at
        old_correlation_id = state.import_correlation_id
        success = AdminStateManager().release_import_lock(active_schema)
        if success:
            logger.info(
                f"[BLUEGREEN] Verrou libéré "
                f"(démarré: {old_started_at}, correlation_id: {old_correlation_id or 'N/A'})"
            )
        return success

    def force_release(self) -> bool:
        """
        Force la libération du verrou sans vérification.

        Utilisé uniquement pour les verrous abandonnés (stale).

        Returns:
            True si libération réussie
        """
        logger.warning("[BLUEGREEN] Libération forcée du verrou")
        return AdminStateManager().force_release_lock()

    def is_stale(self, state: 'BlueGreenState') -> bool:
        """
        Vérifie si le verrou est abandonné (stale).

        Un verrou est considéré comme abandonné si import_started_at
        date de plus de BLUEGREEN_LOCK_TIMEOUT_MINUTES.

        Args:
            state: État à vérifier

        Returns:
            True si le verrou est abandonné
        """
        if not state.import_in_progress or not state.import_started_at:
            return False
        try:
            started_at = datetime.fromisoformat(state.import_started_at)
            timeout = timedelta(minutes=BLUEGREEN_LOCK_TIMEOUT_MINUTES)
            return datetime.now() - started_at > timeout
        except (ValueError, TypeError):
            logger.warning(f"[BLUEGREEN] Date de verrou invalide: {state.import_started_at}")
            return True
