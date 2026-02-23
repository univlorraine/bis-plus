"""
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

L'état est stocké dans la variable Airflow 'amue_bluegreen_state' :
    {
        "last_import_schema": "blue",      # Dernier schéma importé
        "last_switch_timestamp": "...",    # Date du dernier switch
        "last_sync_timestamp": "...",      # Date de la dernière sync
        "import_in_progress": false,       # Flag d'import en cours (verrou concurrent)
        "import_started_at": "...",        # Timestamp ISO du début de l'import
        "import_correlation_id": "...",    # ID de corrélation de l'import en cours
    }

NOTE : Le schéma actif n'est PAS stocké ici. Il est lu dynamiquement
       depuis les vues PostgreSQL (splus.*) via ViewSwitcher.
       Cela évite toute désynchronisation lors d'une restauration de base.

================================================================================
"""
import json
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, replace
from typing import Dict, Optional

from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.config.settings import Defaults
from amue.exceptions import ConcurrentImportError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlueGreenState:
    """État du déploiement blue/green (verrou d'import + audit)"""
    last_import_schema: str = ""
    last_switch_timestamp: str = ""
    last_sync_timestamp: str = ""
    import_in_progress: bool = False
    import_started_at: str = ""  # Timestamp ISO du début de l'import
    import_correlation_id: str = ""  # ID de corrélation de l'import en cours

    def to_dict(self) -> Dict:
        """Convertit l'état en dictionnaire"""
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
        """Crée un état depuis un dictionnaire (ignore les champs inconnus)"""
        return cls(
            last_import_schema=data.get("last_import_schema", ""),
            last_switch_timestamp=data.get("last_switch_timestamp", ""),
            last_sync_timestamp=data.get("last_sync_timestamp", ""),
            import_in_progress=data.get("import_in_progress", False),
            import_started_at=data.get("import_started_at", ""),
            import_correlation_id=data.get("import_correlation_id", ""),
        )


class BlueGreenManager:
    """
    Gestionnaire de l'état blue/green.

    Responsabilités :
        - Lecture/écriture de l'état opérationnel (verrou, audit)
        - Détermination du schéma cible via lecture des vues PostgreSQL
        - Gestion des flags d'import

    Le schéma actif est toujours lu depuis les vues PostgreSQL, jamais
    depuis un état Airflow — ce qui évite les désynchronisations après
    une restauration de base de données.

    Example:
        >>> manager = BlueGreenManager()
        >>> if manager.is_enabled():
        ...     target = manager.get_target_schema()  # 'splus_green'
        ...     manager.mark_import_started()
        ...     # ... import ...
        ...     manager.mark_import_completed()
    """

    # Constantes pour les noms de schémas
    SCHEMA_BLUE = "blue"
    SCHEMA_GREEN = "green"
    SCHEMA_PREFIX = "splus_"
    VIEW_SCHEMA = "splus"

    # Variable Airflow pour l'état
    STATE_VAR_NAME = "amue_bluegreen_state"
    ENABLED_VAR_NAME = "amue_bluegreen_enabled"

    def __init__(self, view_switcher=None):
        """
        Initialise le gestionnaire.

        Args:
            view_switcher: ViewSwitcher injectable (créé lazy si non fourni)
        """
        self._state: Optional[BlueGreenState] = None
        self._view_switcher = view_switcher  # Injectable pour les tests

    @property
    def _vs(self):
        """ViewSwitcher lazy — importé ici pour éviter les imports circulaires."""
        if self._view_switcher is None:
            from amue.services.bluegreen.view_switcher import ViewSwitcher
            self._view_switcher = ViewSwitcher()
        return self._view_switcher

    def _get_active_schema_from_views(self) -> Optional[str]:
        """
        Lit le schéma actif directement depuis les vues PostgreSQL.

        Returns:
            'splus_blue', 'splus_green', ou None si aucune vue n'existe
        """
        return self._vs.get_current_target_schema()

    def is_enabled(self) -> bool:
        """
        Vérifie si le mode blue/green est activé.

        Returns:
            True si amue_bluegreen_enabled est true
        """
        enabled = VarMgr.get(self.ENABLED_VAR_NAME, default="false")
        return str(enabled).lower() == "true"

    def get_state(self) -> BlueGreenState:
        """
        Récupère l'état courant du blue/green.

        Charge l'état depuis la variable Airflow si pas encore chargé.

        Returns:
            BlueGreenState avec les informations courantes
        """
        if self._state is None:
            self._state = self._load_state()
        return self._state

    def _load_state(self) -> BlueGreenState:
        """Charge l'état depuis la variable Airflow"""
        try:
            state_json = VarMgr.get(self.STATE_VAR_NAME, default="{}")
            state_dict = json.loads(state_json) if isinstance(state_json, str) else state_json
            return BlueGreenState.from_dict(state_dict)
        except Exception as e:
            logger.warning(f"[BLUEGREEN] Erreur chargement état: {e}, utilisation état par défaut")
            return BlueGreenState()

    def _save_state(self, state: BlueGreenState) -> bool:
        """
        Sauvegarde l'état dans la variable Airflow.

        Args:
            state: État à sauvegarder

        Returns:
            True si sauvegarde réussie
        """
        try:
            state_json = json.dumps(state.to_dict())
            success = VarMgr.set(self.STATE_VAR_NAME, state_json)
            if success:
                self._state = state
                logger.info(f"[BLUEGREEN] État sauvegardé: {state.to_dict()}")
            return success
        except Exception as e:
            logger.error(f"[BLUEGREEN] Erreur sauvegarde état: {e}")
            return False

    def get_target_schema(self) -> str:
        """
        Retourne le nom complet du schéma cible pour l'import.

        Le schéma cible est toujours l'opposé du schéma actif lu depuis les vues.
        Si aucune vue n'existe (premier import), retourne splus_blue.

        Returns:
            Nom du schéma cible (ex: 'splus_green')
        """
        active = self._get_active_schema_from_views()
        if active is None:
            # Aucune vue → premier import → blue est la cible
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"
        if active == f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}":
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"

    def get_active_schema(self) -> str:
        """
        Retourne le nom complet du schéma actif (lu depuis les vues PostgreSQL).

        Si aucune vue n'existe, retourne splus_green (virtuel) afin que
        get_target_schema() retourne splus_blue pour le premier import.

        Returns:
            Nom du schéma actif (ex: 'splus_blue')
        """
        active = self._get_active_schema_from_views()
        if active is None:
            # Pas de vues → on retourne green (virtuel) pour que target = blue
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return active

    def get_inactive_schema(self) -> str:
        """
        Retourne le nom complet du schéma inactif (opposé de l'actif).

        Returns:
            Nom du schéma inactif (ex: 'splus_green')
        """
        active = self.get_active_schema()
        if active == f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}":
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"

    def get_view_schema(self) -> str:
        """
        Retourne le nom du schéma contenant les vues.

        Returns:
            'splus'
        """
        return self.VIEW_SCHEMA

    def mark_import_started(self, correlation_id: str = "") -> bool:
        """
        Marque le début d'un import avec vérification de concurrence.

        IMPORTANT: Cette méthode lève ConcurrentImportError si un import
        est déjà en cours. Utilisez acquire_import_lock() pour une gestion
        plus fine des verrous.

        Met à jour les flags :
            - import_in_progress = True
            - import_started_at = maintenant
            - import_correlation_id = correlation_id

        Args:
            correlation_id: ID de corrélation pour tracer l'import

        Returns:
            True si mise à jour réussie

        Raises:
            ConcurrentImportError: Si un import est déjà en cours
        """
        return self.acquire_import_lock(correlation_id)

    def mark_import_completed(self) -> bool:
        """
        Marque la fin d'un import (avant switch).

        Libère le verrou d'import et met à jour les flags :
            - import_in_progress = False
            - import_started_at = ""
            - last_import_schema = schéma cible

        Returns:
            True si mise à jour réussie
        """
        target_schema = self.get_target_schema()
        success = self.release_import_lock(mark_completed=True)
        if success:
            logger.info(f"[BLUEGREEN] Import terminé dans {target_schema}")
        return success

    def mark_switch_completed(self) -> bool:
        """
        Marque la fin d'un switch de vues.

        Les vues ont déjà été basculées par ViewSwitcher.
        Met uniquement à jour les flags opérationnels :
            - last_switch_timestamp = maintenant

        Returns:
            True si mise à jour réussie
        """
        state = self.get_state()
        new_state = replace(
            state,
            last_switch_timestamp=datetime.now().isoformat()
        )
        logger.info("[BLUEGREEN] Switch effectué")
        return self._save_state(new_state)

    def mark_sync_completed(self) -> bool:
        """
        Marque la fin d'une synchronisation.

        Met à jour les flags :
            - last_sync_timestamp = maintenant

        Returns:
            True si mise à jour réussie
        """
        state = self.get_state()
        new_state = replace(
            state,
            last_sync_timestamp=datetime.now().isoformat(),
        )
        logger.info("[BLUEGREEN] Sync terminée")
        return self._save_state(new_state)

    def is_import_in_progress(self) -> bool:
        """
        Vérifie si un import est en cours.

        Returns:
            True si import_in_progress est True
        """
        return self.get_state().import_in_progress

    def get_schema_for_table(self, table_name: str) -> str:
        """
        Retourne le nom complet de la table dans le schéma cible.

        Args:
            table_name: Nom de la table

        Returns:
            Nom qualifié (ex: 'splus_green.csks')
        """
        return f"{self.get_target_schema()}.{table_name.lower()}"

    def needs_sync(self) -> bool:
        """
        Vérifie si une synchronisation est nécessaire avant import.

        La sync est nécessaire si c'est le premier import après activation
        (pas encore de sync enregistrée).

        Returns:
            True si sync nécessaire
        """
        state = self.get_state()
        return not state.last_sync_timestamp

    def reset_state(self) -> bool:
        """
        Réinitialise l'état à ses valeurs par défaut.

        Utile pour les tests ou en cas de corruption de l'état.

        Returns:
            True si réinitialisation réussie
        """
        logger.info("[BLUEGREEN] Réinitialisation de l'état")
        return self._save_state(BlueGreenState())

    # =========================================================================
    # GESTION DU VERROUILLAGE CONCURRENT
    # =========================================================================

    def acquire_import_lock(self, correlation_id: str = "") -> bool:
        """
        Acquiert un verrou exclusif pour l'import.

        Cette méthode vérifie qu'aucun autre import n'est en cours avant
        de marquer l'import comme démarré. Elle gère également les verrous
        abandonnés (stale locks) en les libérant automatiquement.

        Args:
            correlation_id: ID de corrélation pour tracer l'import

        Returns:
            True si le verrou a été acquis

        Raises:
            ConcurrentImportError: Si un autre import est en cours
        """
        # Recharge l'état depuis Airflow (pas le cache)
        self._state = None
        state = self.get_state()

        if state.import_in_progress:
            # Vérifie si le verrou est abandonné (stale)
            if self._is_lock_stale(state):
                logger.warning(
                    f"[BLUEGREEN] Verrou abandonné détecté (démarré: {state.import_started_at}). "
                    f"Libération automatique."
                )
                self._force_release_lock()
                # Recharge l'état
                self._state = None
                state = self.get_state()
            else:
                # Import réellement en cours
                raise ConcurrentImportError(
                    f"Un import est déjà en cours depuis {state.import_started_at}",
                    import_started_at=state.import_started_at,
                    context={
                        "correlation_id": state.import_correlation_id,
                        "target_schema": self.get_target_schema()
                    }
                )

        # Acquérir le verrou
        new_state = replace(
            state,
            import_in_progress=True,
            import_started_at=datetime.now().isoformat(),
            import_correlation_id=correlation_id
        )

        if self._save_state(new_state):
            logger.info(
                f"[BLUEGREEN] Verrou acquis pour import "
                f"(correlation_id: {correlation_id or 'N/A'})"
            )
            return True

        return False

    def release_import_lock(self, mark_completed: bool = True) -> bool:
        """
        Libère le verrou d'import.

        Args:
            mark_completed: Si True, marque l'import comme terminé avec succès

        Returns:
            True si le verrou a été libéré
        """
        state = self.get_state()

        if not state.import_in_progress:
            logger.warning("[BLUEGREEN] Tentative de libération d'un verrou inexistant")
            return True  # Pas d'erreur, le verrou est déjà libéré

        old_started_at = state.import_started_at
        old_correlation_id = state.import_correlation_id

        last_import = state.last_import_schema
        if mark_completed:
            # À ce stade les vues pointent encore vers l'ancien actif
            # → get_target_schema() retourne le schéma qui vient d'être importé
            last_import = self.get_target_schema().replace(self.SCHEMA_PREFIX, "")

        new_state = replace(
            state,
            import_in_progress=False,
            last_import_schema=last_import,
            import_started_at="",
            import_correlation_id=""
        )

        if self._save_state(new_state):
            logger.info(
                f"[BLUEGREEN] Verrou libéré "
                f"(démarré: {old_started_at}, correlation_id: {old_correlation_id or 'N/A'})"
            )
            return True

        return False

    def _is_lock_stale(self, state: BlueGreenState) -> bool:
        """
        Vérifie si le verrou est abandonné (stale).

        Un verrou est considéré comme abandonné si :
            - import_in_progress est True
            - import_started_at date de plus de BLUEGREEN_LOCK_TIMEOUT_MINUTES

        Args:
            state: État à vérifier

        Returns:
            True si le verrou est abandonné
        """
        if not state.import_in_progress or not state.import_started_at:
            return False

        try:
            started_at = datetime.fromisoformat(state.import_started_at)
            timeout = timedelta(minutes=Defaults.BLUEGREEN_LOCK_TIMEOUT_MINUTES)
            return datetime.now() - started_at > timeout
        except (ValueError, TypeError):
            # Si la date est invalide, considère le verrou comme stale
            logger.warning(f"[BLUEGREEN] Date de verrou invalide: {state.import_started_at}")
            return True

    def _force_release_lock(self) -> bool:
        """
        Force la libération du verrou sans vérification.

        Utilisé uniquement pour les verrous abandonnés.

        Returns:
            True si libération réussie
        """
        state = self.get_state()
        new_state = replace(
            state,
            import_in_progress=False,
            import_started_at="",
            import_correlation_id=""
        )
        logger.warning("[BLUEGREEN] Libération forcée du verrou")
        return self._save_state(new_state)

    def get_lock_info(self) -> Optional[Dict]:
        """
        Retourne les informations sur le verrou actuel.

        Returns:
            Dict avec les infos du verrou ou None si pas de verrou
        """
        state = self.get_state()

        if not state.import_in_progress:
            return None

        return {
            "import_in_progress": state.import_in_progress,
            "import_started_at": state.import_started_at,
            "import_correlation_id": state.import_correlation_id,
            "is_stale": self._is_lock_stale(state),
            "target_schema": self.get_target_schema()
        }
