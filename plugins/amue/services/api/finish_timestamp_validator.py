"""
Validation du timestamp finish retourné par l'API AMUE.

Ce module détermine si un import doit être exécuté en comparant
le timestamp finish actuel avec celui stocké en base.
"""
import logging

logger = logging.getLogger(__name__)

# Valeurs considérées comme invalides pour le finish timestamp
_INVALID_VALUES = {'', 'null', 'none', 'undefined', '0', '00000000'}


class FinishTimestampValidator:
    """
    Valide le timestamp finish et détermine si l'import doit être ignoré.

    Logique :
        - Premiere exécution (pas de timestamp stocké) → import exécuté
        - Force import activé (amue_force_import=true) → import toujours exécuté
        - Même timestamp → import ignoré (pas de nouvelles données)
        - Timestamp supérieur → import exécuté (nouvelles données)
        - Timestamp inférieur → import ignoré (cas anormal)

    Example:
        >>> validator = FinishTimestampValidator()
        >>> validator.validate('2026-03-09T10:00:00')
        True
        >>> validator.should_skip('2026-03-09T10:00:00')
        False
    """

    def validate(self, finish_value: str) -> bool:
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

        if finish_value.lower().strip() in _INVALID_VALUES:
            logger.warning(f"[POLLING] Finish timestamp invalide: '{finish_value}'")
            return False

        logger.info(f"[POLLING] Finish timestamp valide: {finish_value}")
        return True

    def should_skip(self, current_finish: str) -> bool:
        """
        Détermine si l'import doit être ignoré (timestamp inchangé ou invalide).

        Note: La vérification de force_import est gérée par l'appelant
        (AMUEPollingService._should_skip_import).

        Args:
            current_finish: Timestamp finish retourné par l'API

        Returns:
            True si l'import doit être ignoré
        """
        if not self.validate(current_finish):
            logger.warning("[POLLING] Finish invalide - import exécuté par précaution")
            return False

        from amue.services.admin_state_manager import AdminStateManager
        stored_finish = AdminStateManager().get_last_finish_timestamp() or ''

        if not stored_finish or not stored_finish.strip():
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] PREMIÈRE EXÉCUTION DÉTECTÉE")
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] Aucun timestamp précédent enregistré")
            logger.info(f"[POLLING] Timestamp actuel de l'API: {current_finish}")
            logger.info("[POLLING] L'import sera exécuté et ce timestamp sera sauvegardé")
            return False

        if current_finish > stored_finish:
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] NOUVEAU TIMESTAMP DÉTECTÉ")
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info(f"[POLLING] Timestamp précédent: {stored_finish}")
            logger.info(f"[POLLING] Timestamp actuel:    {current_finish}")
            logger.info("[POLLING] De nouvelles données sont disponibles")
            return False

        if stored_finish == current_finish:
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info("[POLLING] TIMESTAMP INCHANGÉ")
            logger.info("[POLLING] ═══════════════════════════════════════════")
            logger.info(f"[POLLING] Timestamp stocké:  {stored_finish}")
            logger.info(f"[POLLING] Timestamp actuel:  {current_finish}")
            logger.info("[POLLING] Pas de nouvelles données disponibles")
            logger.info("[POLLING] Pour forcer l'import: amue_force_import=true")
            return True

        # Timestamp inférieur → cas anormal
        logger.warning("[POLLING] ═══════════════════════════════════════════")
        logger.warning("[POLLING] TIMESTAMP INFÉRIEUR AU PRÉCÉDENT (ANORMAL)")
        logger.warning("[POLLING] ═══════════════════════════════════════════")
        logger.warning(f"[POLLING] Timestamp précédent: {stored_finish}")
        logger.warning(f"[POLLING] Timestamp actuel:    {current_finish}")
        logger.warning("[POLLING] Cas anormal - import ignoré par sécurité")
        logger.warning("[POLLING] Pour forcer l'import: amue_force_import=true")
        return True
