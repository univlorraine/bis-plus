"""
Service de rollback pour l'architecture Blue/Green.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module permet de revenir rapidement à l'état précédent en cas de
problème avec les données importées. Le rollback est instantané car
il consiste simplement à reswitcher les vues vers l'ancien schéma.

ROLLBACK :
    - Instantané (< 1 seconde)
    - Consiste à switcher les vues vers le schéma inactif
    - Disponible jusqu'au prochain DAG run (sync écrase le snapshot)

================================================================================
CONDITIONS DE ROLLBACK
================================================================================

Le rollback n'est possible que si :
    1. Le mode blue/green est activé
    2. rollback_available = True dans l'état
    3. Aucun import n'est en cours

Une fois la sync effectuée (début du prochain DAG), le rollback
devient impossible car le snapshot a été écrasé.

================================================================================
"""
import logging
from typing import Dict, Optional

from amue.services.bluegreen_manager import BlueGreenManager
from amue.services.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


class RollbackManager:
    """
    Gère le rollback vers l'état précédent.

    Le rollback permet de revenir instantanément aux données du schéma
    inactif (snapshot de l'import précédent) en cas de problème.

    Example:
        >>> manager = RollbackManager()
        >>> if manager.can_rollback():
        ...     result = manager.rollback()
        ...     if result['success']:
        ...         print("Rollback effectué")
    """

    def __init__(
        self,
        bluegreen_manager: BlueGreenManager = None,
        view_switcher: ViewSwitcher = None
    ):
        """
        Initialise le RollbackManager.

        Args:
            bluegreen_manager: Gestionnaire d'état (créé si non fourni)
            view_switcher: Switcher de vues (créé si non fourni)
        """
        self.bluegreen_manager = bluegreen_manager or BlueGreenManager()
        self.view_switcher = view_switcher or ViewSwitcher()

    def can_rollback(self) -> bool:
        """
        Vérifie si un rollback est possible.

        Conditions :
            - Mode blue/green activé
            - rollback_available = True
            - Pas d'import en cours

        Returns:
            True si rollback possible
        """
        if not self.bluegreen_manager.is_enabled():
            logger.warning("[ROLLBACK] Blue/green désactivé")
            return False

        if self.bluegreen_manager.is_import_in_progress():
            logger.warning("[ROLLBACK] Import en cours, rollback impossible")
            return False

        if not self.bluegreen_manager.is_rollback_available():
            logger.warning("[ROLLBACK] Rollback non disponible (sync déjà effectuée)")
            return False

        return True

    def get_rollback_info(self) -> Dict:
        """
        Retourne les informations sur le rollback disponible.

        Returns:
            Informations de rollback :
            {
                'available': True | False,
                'rollback_schema': 'splus_blue',
                'current_schema': 'splus_green',
                'last_switch': '2024-01-15T10:30:00',
                'reason': None | 'message si non disponible'
            }
        """
        state = self.bluegreen_manager.get_state()

        info = {
            'available': False,
            'rollback_schema': None,
            'current_schema': None,
            'last_switch': state.last_switch_timestamp,
            'reason': None
        }

        if not self.bluegreen_manager.is_enabled():
            info['reason'] = "Mode blue/green désactivé"
            return info

        if self.bluegreen_manager.is_import_in_progress():
            info['reason'] = "Import en cours"
            return info

        if not state.rollback_available:
            info['reason'] = "Sync déjà effectuée, snapshot écrasé"
            return info

        # Rollback disponible
        info['available'] = True
        info['rollback_schema'] = f"splus_{state.rollback_schema}"
        info['current_schema'] = f"splus_{state.active_schema}"
        return info

    def rollback(self) -> Dict:
        """
        Effectue le rollback vers le schéma précédent.

        Le rollback :
            1. Vérifie les conditions
            2. Switch les vues vers le schéma inactif
            3. Met à jour l'état blue/green

        Returns:
            Résultat du rollback :
            {
                'success': True | False,
                'previous_schema': 'splus_green',
                'new_schema': 'splus_blue',
                'error': None | 'message'
            }
        """
        logger.info("[ROLLBACK] Tentative de rollback...")

        # Vérifie les conditions
        if not self.can_rollback():
            info = self.get_rollback_info()
            return {
                'success': False,
                'previous_schema': None,
                'new_schema': None,
                'error': info.get('reason', 'Rollback non disponible')
            }

        state = self.bluegreen_manager.get_state()
        rollback_schema = f"splus_{state.rollback_schema}"
        current_schema = f"splus_{state.active_schema}"

        logger.info(f"[ROLLBACK] Switch: {current_schema} -> {rollback_schema}")

        # Switch des vues
        success = self.view_switcher.switch_views_to_schema(rollback_schema)

        if not success:
            return {
                'success': False,
                'previous_schema': current_schema,
                'new_schema': None,
                'error': 'Échec du switch des vues'
            }

        # Met à jour l'état
        self.bluegreen_manager.mark_rollback_completed()

        logger.info(f"[ROLLBACK] SUCCESS - Rollback vers {rollback_schema}")
        return {
            'success': True,
            'previous_schema': current_schema,
            'new_schema': rollback_schema,
            'error': None
        }

    def preview_rollback(self) -> Dict:
        """
        Prévisualise ce que ferait le rollback sans l'exécuter.

        Utile pour afficher les informations avant confirmation.

        Returns:
            Prévisualisation du rollback
        """
        info = self.get_rollback_info()

        preview = {
            'would_rollback': info['available'],
            'from_schema': info.get('current_schema'),
            'to_schema': info.get('rollback_schema'),
            'last_switch': info.get('last_switch'),
            'reason_if_not_available': info.get('reason')
        }

        if info['available']:
            # Ajoute des infos sur les données
            current_schema = info['current_schema']
            rollback_schema = info['rollback_schema']

            logger.info(f"[ROLLBACK] Prévisualisation: {current_schema} -> {rollback_schema}")

        return preview

    def force_rollback(self) -> Dict:
        """
        Force le rollback même si les conditions ne sont pas remplies.

        ATTENTION : À utiliser uniquement en cas d'urgence.
        Cette méthode ignore les vérifications de sécurité.

        Returns:
            Résultat du rollback forcé
        """
        logger.warning("[ROLLBACK] FORCE - Rollback forcé demandé")

        if not self.bluegreen_manager.is_enabled():
            return {
                'success': False,
                'error': 'Mode blue/green désactivé, impossible de forcer'
            }

        state = self.bluegreen_manager.get_state()

        # Détermine le schéma de rollback (opposé de l'actif)
        rollback_schema = 'splus_blue' if state.active_schema == 'green' else 'splus_green'
        current_schema = f"splus_{state.active_schema}"

        logger.warning(f"[ROLLBACK] FORCE - Switch: {current_schema} -> {rollback_schema}")

        # Switch des vues
        success = self.view_switcher.switch_views_to_schema(rollback_schema)

        if not success:
            return {
                'success': False,
                'previous_schema': current_schema,
                'new_schema': None,
                'error': 'Échec du switch des vues (force)'
            }

        # Met à jour l'état manuellement
        state.active_schema = rollback_schema.replace('splus_', '')
        state.inactive_schema = current_schema.replace('splus_', '')
        state.rollback_available = False
        self.bluegreen_manager._save_state(state)

        logger.warning(f"[ROLLBACK] FORCE - SUCCESS - Rollback vers {rollback_schema}")
        return {
            'success': True,
            'previous_schema': current_schema,
            'new_schema': rollback_schema,
            'error': None,
            'forced': True
        }

    def verify_rollback_integrity(self) -> Dict:
        """
        Vérifie l'intégrité après un rollback.

        Contrôle que les vues pointent vers le bon schéma.

        Returns:
            Résultat de la vérification
        """
        state = self.bluegreen_manager.get_state()
        expected_schema = f"splus_{state.active_schema}"

        views_ok = self.view_switcher.verify_views_point_to(expected_schema)

        return {
            'verified': views_ok,
            'expected_schema': expected_schema,
            'actual_schema': self.view_switcher.get_current_target_schema()
        }
