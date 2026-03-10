"""
Résolution des noms de schémas blue/green depuis les vues PostgreSQL.

Le schéma actif est toujours lu depuis les vues (pas depuis l'état BDD),
ce qui évite les désynchronisations après restauration de base.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BlueGreenSchemaResolver:
    """
    Détermine les schémas actif et cible en lisant les vues PostgreSQL.

    Responsabilités :
        - Lire le schéma actif depuis les vues via ViewSwitcher
        - Calculer le schéma cible (opposé de l'actif)
        - Fournir les noms de schémas (actif, inactif, vues)

    Example:
        >>> resolver = BlueGreenSchemaResolver()
        >>> resolver.get_target_schema()  # 'splus_green' si actif=blue
        >>> resolver.get_active_schema()  # 'splus_blue'
    """

    SCHEMA_BLUE = "blue"
    SCHEMA_GREEN = "green"
    SCHEMA_PREFIX = "splus_"
    VIEW_SCHEMA = "splus"

    def __init__(self, view_switcher=None):
        """
        Args:
            view_switcher: ViewSwitcher injectable (créé lazy si non fourni)
        """
        self._view_switcher = view_switcher

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

    def get_target_schema(self) -> str:
        """
        Retourne le schéma cible pour l'import (opposé de l'actif).

        Si aucune vue n'existe (premier import), retourne splus_blue.

        Returns:
            Nom du schéma cible (ex: 'splus_green')
        """
        active = self._get_active_schema_from_views()
        if active is None:
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"
        if active == f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}":
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"

    def get_active_schema(self) -> str:
        """
        Retourne le schéma actif (lu depuis les vues PostgreSQL).

        Si aucune vue n'existe, retourne splus_green afin que
        get_target_schema() retourne splus_blue pour le premier import.

        Returns:
            Nom du schéma actif (ex: 'splus_blue')
        """
        active = self._get_active_schema_from_views()
        if active is None:
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return active

    def get_inactive_schema(self) -> str:
        """
        Retourne le schéma inactif (opposé de l'actif).

        Returns:
            Nom du schéma inactif (ex: 'splus_green')
        """
        active = self.get_active_schema()
        if active == f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}":
            return f"{self.SCHEMA_PREFIX}{self.SCHEMA_GREEN}"
        return f"{self.SCHEMA_PREFIX}{self.SCHEMA_BLUE}"

    def get_view_schema(self) -> str:
        """Retourne le nom du schéma contenant les vues."""
        return self.VIEW_SCHEMA
