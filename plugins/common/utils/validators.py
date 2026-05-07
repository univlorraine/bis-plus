"""
Validation d'identifiants SQL (tables, colonnes, schémas).

Module générique partagé AMUE / ECC. Protège contre les injections SQL
en validant les identifiants avant de les interpoler dans des requêtes
DDL/DML.
"""
import re

_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_]{1,63}$')


def validate_table_name(table_name: str) -> str:
    """
    Valide et normalise un nom de table.

    Returns:
        Nom de table en majuscules.

    Raises:
        ValueError: Si le nom est invalide.

    Example:
        >>> validate_table_name('csks')
        'CSKS'
    """
    if not table_name:
        raise ValueError("Le nom de table ne peut pas être vide")
    table_name = table_name.strip()
    if not _IDENTIFIER_RE.match(table_name):
        raise ValueError(
            f"Nom de table invalide: '{table_name}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés (max 63 caractères)."
        )
    return table_name.upper()


def validate_column_name(column_name: str) -> str:
    """
    Valide et normalise un nom de colonne.

    Returns:
        Nom de colonne en minuscules.
    """
    if not column_name:
        raise ValueError("Le nom de colonne ne peut pas être vide")
    column_name = column_name.strip()
    if not _IDENTIFIER_RE.match(column_name):
        raise ValueError(
            f"Nom de colonne invalide: '{column_name}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés (max 63 caractères)."
        )
    return column_name.lower()


def validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Valide un identifiant SQL générique sans normalisation de casse.

    Args:
        identifier: Identifiant à valider.
        identifier_type: Type d'identifiant (pour le message d'erreur).
    """
    if not identifier:
        raise ValueError(f"Le {identifier_type} ne peut pas être vide")
    identifier = identifier.strip()
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(
            f"{identifier_type.capitalize()} invalide: '{identifier}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés."
        )
    return identifier
