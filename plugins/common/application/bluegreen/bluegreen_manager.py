"""
Layer: application

Gestionnaire d'état Blue/Green pour l'import AMUE.

================================================================================
ARCHITECTURE BLUE/GREEN
================================================================================

Ce module gère l'alternance entre deux schémas identiques (blue et green)
pour permettre des imports atomiques.

SCHÉMAS :
    - splus_blue  : Tables blue
    - splus_green : Tables green (identiques)
    - splus       : Vues pointant vers le schéma actif

WORKFLOW :
    1. Déterminer le schéma cible (opposé de l'actif lu depuis les vues)
    2. Synchroniser si nécessaire (copie schéma actif -> cible)
    3. Importer les données dans le schéma cible
    4. Switcher les vues vers le nouveau schéma

================================================================================
ÉTAT BLUE/GREEN
================================================================================

L'état est stocké dans la table PostgreSQL splus_admin.amue_state :
    last_import_schema    (active_schema)
    last_switch_timestamp
    last_sync_timestamp
    import_in_progress    (verrou concurrent)
    import_started_at
    import_correlation_id

NOTE : Le schéma actif n'est PAS l'unique source de vérité ici. Il est lu
       dynamiquement depuis les vues PostgreSQL (splus.*) via ViewSwitcher.
       Cela évite toute désynchronisation lors d'une restauration de base.

================================================================================
ARCHITECTURE INTERNE (après refactorisation)
================================================================================

BlueGreenManager est la façade publique qui compose :
    - BlueGreenSchemaResolver : résolution des noms de schémas (target, active, inactive)
    - BlueGreenStateManager   : chargement/sauvegarde de l'état en BDD
    - BlueGreenLockManager    : verrou exclusif d'import (atomique PostgreSQL)

BlueGreenState (dataclass) est défini dans bluegreen_state_manager.py et
re-exporté ici pour la rétrocompatibilité.

================================================================================
"""
import logging
from typing import Dict, Optional

from common.domain.interfaces import SqlExecutor
from common.infrastructure.database.schema_introspection import schema_exists as _schema_exists
from common.application.bluegreen.bluegreen_state_manager import BlueGreenState, BlueGreenStateManager
from common.application.bluegreen.bluegreen_schema_resolver import BlueGreenSchemaResolver
from common.application.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

# Re-export BlueGreenState pour la rétrocompatibilité des imports existants
__all__ = ['BlueGreenManager', 'BlueGreenState']

logger = logging.getLogger(__name__)


class BlueGreenManager:
    """
    Façade orchestrant les composants du mode blue/green.

    Responsabilités :
        - Déléguer la résolution de schéma à BlueGreenSchemaResolver
        - Déléguer la persistance d'état à BlueGreenStateManager
        - Déléguer la gestion du verrou à BlueGreenLockManager
        - Gérer le cache local de l'état (_state)
        - Gérer les renommages DDL (rename_schema_to/from_offline)

    Example:
        >>> manager = BlueGreenManager()
        >>> target = manager.get_target_schema()  # 'splus_green'
        >>> manager.mark_import_started()
        >>> # ... import ...
        >>> manager.mark_import_completed()
    """

    # Constantes pour les noms de schémas (délégués au resolver)
    SCHEMA_BLUE = BlueGreenSchemaResolver.SCHEMA_BLUE
    SCHEMA_GREEN = BlueGreenSchemaResolver.SCHEMA_GREEN
    SCHEMA_PREFIX = BlueGreenSchemaResolver.SCHEMA_PREFIX
    VIEW_SCHEMA = BlueGreenSchemaResolver.VIEW_SCHEMA
    OFFLINE_SUFFIX = "_offline"

    def __init__(self, view_switcher=None, postgres_hook=None):
        """
        Initialise le gestionnaire.

        Args:
            view_switcher: ViewSwitcher injectable (créé lazy si non fourni)
            postgres_hook: Hook PostgreSQL injectable (créé lazy si non fourni)
        """
        self._state: Optional[BlueGreenState] = None
        self._postgres_hook = postgres_hook

        # Sous-composants
        self._resolver = BlueGreenSchemaResolver(view_switcher)
        self._state_mgr = BlueGreenStateManager()
        self._lock_mgr = BlueGreenLockManager()

    @property
    def _view_switcher(self):
        """Proxy vers le view_switcher du resolver (rétrocompatibilité tests)."""
        return self._resolver._view_switcher

    @_view_switcher.setter
    def _view_switcher(self, value):
        """Proxy setter vers le resolver (permet l'injection dans les tests)."""
        self._resolver._view_switcher = value

    @property
    def _hook(self) -> SqlExecutor:
        """Hook PostgreSQL lazy pour les opérations DDL sur les schémas."""
        if self._postgres_hook is None:
            from common.infrastructure.database.hooks import create_postgres_hook
            self._postgres_hook = create_postgres_hook(schema='public')
        return self._postgres_hook

    # =========================================================================
    # GESTION DE L'ÉTAT (avec cache local)
    # =========================================================================

    def get_state(self) -> BlueGreenState:
        """
        Récupère l'état courant du blue/green (avec cache local).

        Returns:
            BlueGreenState avec les informations courantes
        """
        if self._state is None:
            self._state = self._state_mgr.load_state()
        return self._state

    def _load_state(self) -> BlueGreenState:
        """Charge l'état depuis la BDD (sans cache)."""
        return self._state_mgr.load_state()

    def _save_state(self, state: BlueGreenState) -> bool:
        """Sauvegarde l'état dans la BDD et met à jour le cache."""
        success = self._state_mgr.save_state(state)
        if success:
            self._state = state
        return success

    # =========================================================================
    # RÉSOLUTION DES SCHÉMAS (délégué à BlueGreenSchemaResolver)
    # =========================================================================

    def _get_active_schema_from_views(self) -> Optional[str]:
        """Lit le schéma actif depuis les vues PostgreSQL."""
        return self._resolver._get_active_schema_from_views()

    def get_target_schema(self) -> str:
        """Retourne le schéma cible pour l'import (opposé de l'actif)."""
        return self._resolver.get_target_schema()

    def get_active_schema(self) -> str:
        """Retourne le schéma actif (lu depuis les vues PostgreSQL)."""
        return self._resolver.get_active_schema()

    def get_inactive_schema(self) -> str:
        """Retourne le schéma inactif (opposé de l'actif)."""
        return self._resolver.get_inactive_schema()

    def get_view_schema(self) -> str:
        """Retourne le nom du schéma contenant les vues."""
        return self._resolver.get_view_schema()

    # =========================================================================
    # MARQUAGE D'ÉTAT (mark_*)
    # =========================================================================

    def mark_import_started(self, correlation_id: str = "") -> bool:
        """
        Marque le début d'un import avec vérification de concurrence.

        Raises:
            ConcurrentImportError: Si un import est déjà en cours
        """
        return self.acquire_import_lock(correlation_id)

    def mark_import_completed(self, target_schema: str = None) -> bool:
        """
        Marque la fin d'un import (avant switch).

        Libère le verrou et enregistre le schéma cible.
        """
        if target_schema is None:
            target_schema = self.get_target_schema()
        active_schema_short = target_schema.replace(self.SCHEMA_PREFIX, "")
        success = self._lock_mgr.release_lock(active_schema_short, self.get_state())
        if success:
            self._state = None
            logger.info(f"[BLUEGREEN] Import terminé dans {target_schema}")
        return success

    def mark_switch_completed(self) -> bool:
        """
        Marque la fin d'un switch de vues.

        Returns:
            True si mise à jour réussie
        """
        new_active = self.get_active_schema().replace(self.SCHEMA_PREFIX, "")
        success = self._state_mgr.mark_switch_completed(new_active)
        if success:
            self._state = None
        return success

    def mark_sync_completed(self) -> bool:
        """
        Marque la fin d'une synchronisation.

        Returns:
            True si mise à jour réussie
        """
        success = self._state_mgr.mark_sync_completed()
        if success:
            self._state = None
        return success

    # =========================================================================
    # MÉTHODES DE COMMODITÉ
    # =========================================================================

    def is_import_in_progress(self) -> bool:
        """Vérifie si un import est en cours."""
        return self.get_state().import_in_progress

    def get_schema_for_table(self, table_name: str) -> str:
        """Retourne le nom qualifié de la table dans le schéma cible."""
        return f"{self.get_target_schema()}.{table_name.lower()}"

    def needs_sync(self) -> bool:
        """Vérifie si une synchronisation est nécessaire avant import."""
        state = self.get_state()
        return not state.last_sync_timestamp

    def reset_state(self) -> bool:
        """Réinitialise l'état à ses valeurs par défaut."""
        logger.info("[BLUEGREEN] Réinitialisation de l'état")
        return self._save_state(BlueGreenState())

    # =========================================================================
    # GESTION DU VERROUILLAGE CONCURRENT (délégué à BlueGreenLockManager)
    # =========================================================================

    def acquire_import_lock(self, correlation_id: str = "") -> bool:
        """
        Acquiert un verrou exclusif pour l'import.

        Raises:
            ConcurrentImportError: Si un autre import est en cours
        """
        self._state = None  # Rafraîchit l'état avant lecture
        state = self.get_state()
        result = self._lock_mgr.acquire_lock(correlation_id, state)
        self._state = None  # Invalide le cache après mutation
        return result

    def release_import_lock(self, mark_completed: bool = True) -> bool:
        """Libère le verrou d'import."""
        state = self.get_state()

        if not state.import_in_progress:
            logger.warning("[BLUEGREEN] Tentative de libération d'un verrou inexistant")
            return True

        active_schema = ""
        if mark_completed:
            active_schema = self.get_target_schema().replace(self.SCHEMA_PREFIX, "")

        success = self._lock_mgr.release_lock(active_schema, state)
        if success:
            self._state = None
        return success

    def _release_lock_with_schema(self, active_schema: str) -> bool:
        """Libère le verrou avec le schéma actif fourni explicitement."""
        state = self.get_state()
        success = self._lock_mgr.release_lock(active_schema, state)
        if success:
            self._state = None
        return success

    def _is_lock_stale(self, state: BlueGreenState) -> bool:
        """Vérifie si le verrou est abandonné (stale)."""
        return self._lock_mgr.is_stale(state)

    def _force_release_lock(self) -> bool:
        """Force la libération du verrou sans vérification."""
        success = self._lock_mgr.force_release()
        if success:
            self._state = None
        return success

    # =========================================================================
    # GESTION DU RENOMMAGE OFFLINE
    # =========================================================================

    def schema_exists(self, schema_name: str) -> bool:
        """Vérifie si un schéma PostgreSQL existe."""
        return _schema_exists(self._hook, schema_name)

    def rename_schema_to_offline(self, schema_name: str) -> bool:
        """
        Renomme un schéma en ajoutant le suffixe _offline.

        Appelé après un switch de vues réussi.
        """
        offline_name = f"{schema_name}{self.OFFLINE_SUFFIX}"
        if not self.schema_exists(schema_name):
            logger.warning(f"[BLUEGREEN] Schéma {schema_name!r} introuvable, rename ignoré")
            return False
        logger.info(f"[BLUEGREEN] Renommage schéma: {schema_name!r} → {offline_name!r}")
        self._hook.run(f'ALTER SCHEMA "{schema_name}" RENAME TO "{offline_name}"')
        logger.info(f"[BLUEGREEN] Schéma renommé: {schema_name!r} → {offline_name!r}")
        return True

    def rename_schema_from_offline(self, schema_name: str) -> bool:
        """
        Restaure un schéma offline à son nom de base.

        Appelé avant un import ou une sync.
        """
        offline_name = f"{schema_name}{self.OFFLINE_SUFFIX}"
        if not self.schema_exists(offline_name):
            return False
        self._hook.run(f'ALTER SCHEMA "{offline_name}" RENAME TO "{schema_name}"')
        logger.info(f"[BLUEGREEN] Schéma restauré: {offline_name!r} → {schema_name!r}")
        return True

    def get_lock_info(self) -> Optional[Dict]:
        """Retourne les informations sur le verrou actuel."""
        state = self.get_state()

        if not state.import_in_progress:
            return None

        return {
            "import_in_progress": state.import_in_progress,
            "import_started_at": state.import_started_at,
            "import_correlation_id": state.import_correlation_id,
            "is_stale": self._lock_mgr.is_stale(state),
            "target_schema": self.get_target_schema()
        }
