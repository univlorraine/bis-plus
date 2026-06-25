"""
Layer: infrastructure

Qualification des noms de tables avec leur schéma PostgreSQL (blue/green).

================================================================================
RÔLE DU MODULE
================================================================================

Ce module fournit une classe utilitaire pour gérer la qualification des noms
de tables avec leur schéma PostgreSQL. Il élimine la duplication de code
présente dans plusieurs modules (batch_upserter, table_manager, view_switcher).

USAGE :
    >>> qualifier = SchemaQualifier('splus_blue')
    >>> qualifier.qualify('CSKS')
    'splus_blue.csks'
    >>> qualifier.qualify_identifier('CSKS')
    Composed([Identifier('splus_blue'), SQL('.'), Identifier('csks')])

================================================================================
"""
from typing import Optional

from psycopg2 import sql


class SchemaQualifier:
    """
    Utilitaire centralisé pour la qualification des noms de tables.

    Cette classe encapsule la logique de qualification des noms de tables
    avec leur schéma, garantissant une cohérence dans tout le projet.

    Attributes:
        target_schema: Schéma cible pour la qualification (ex: 'splus_blue')

    Example:
        >>> qualifier = SchemaQualifier('splus_blue')
        >>> qualifier.qualify('csks')
        'splus_blue.csks'

        >>> # Sans schéma (mode standard)
        >>> qualifier = SchemaQualifier()
        >>> qualifier.qualify('csks')
        'csks'
    """

    def __init__(self, target_schema: Optional[str] = None):
        """
        Initialise le qualificateur de schéma.

        Args:
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue').
                          Si None, les noms de tables ne seront pas préfixés.
        """
        self._target_schema = target_schema

    @property
    def target_schema(self) -> Optional[str]:
        """Retourne le schéma cible."""
        return self._target_schema

    @target_schema.setter
    def target_schema(self, value: Optional[str]) -> None:
        """Définit le schéma cible."""
        self._target_schema = value

    def qualify(self, table_name: str) -> str:
        """
        Retourne le nom de table qualifié avec le schéma.

        Le nom de table est toujours converti en minuscules pour
        respecter les conventions PostgreSQL.

        Args:
            table_name: Nom de la table (ex: 'CSKS', 'csks')

        Returns:
            Nom qualifié (ex: 'splus_blue.csks' ou 'csks' si pas de schéma)

        Example:
            >>> qualifier = SchemaQualifier('splus_green')
            >>> qualifier.qualify('PRPS')
            'splus_green.prps'
        """
        table_lower = table_name.lower()
        if self._target_schema:
            return f"{self._target_schema}.{table_lower}"
        return table_lower

    def qualify_identifier(self, table_name: str) -> sql.Composable:
        """
        Retourne un identifiant SQL sécurisé et qualifié.

        Utilise psycopg2.sql pour construire un identifiant sécurisé
        qui prévient les injections SQL.

        Args:
            table_name: Nom de la table

        Returns:
            Identifiant SQL composé (Composed ou Identifier)

        Example:
            >>> qualifier = SchemaQualifier('splus_blue')
            >>> qualifier.qualify_identifier('csks')
            Composed([Identifier('splus_blue'), SQL('.'), Identifier('csks')])
        """
        table_lower = table_name.lower()
        if self._target_schema:
            return sql.SQL("{}.{}").format(
                sql.Identifier(self._target_schema),
                sql.Identifier(table_lower)
            )
        return sql.Identifier(table_lower)

    def unqualify(self, qualified_name: str) -> str:
        """
        Extrait le nom de table sans le schéma.

        Args:
            qualified_name: Nom qualifié (ex: 'splus_blue.csks')

        Returns:
            Nom de table seul (ex: 'csks')

        Example:
            >>> qualifier = SchemaQualifier('splus_blue')
            >>> qualifier.unqualify('splus_blue.csks')
            'csks'
        """
        if '.' in qualified_name:
            return qualified_name.split('.', 1)[1].lower()
        return qualified_name.lower()

    def is_qualified(self, name: str) -> bool:
        """
        Vérifie si un nom est déjà qualifié avec un schéma.

        Args:
            name: Nom à vérifier

        Returns:
            True si le nom contient un point (schéma.table)
        """
        return '.' in name

    def __repr__(self) -> str:
        """Représentation string de l'instance."""
        return f"SchemaQualifier(target_schema={self._target_schema!r})"
