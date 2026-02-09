"""
Fonctions de transformation et validation pour les données AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module contient des fonctions utilitaires de trois catégories :

1. VALIDATION (protection contre les injections SQL)
2. TRANSFORMATION DE TYPES (SQLite/AMUE → PostgreSQL)
3. FINGERPRINT (empreinte de structure pour détection des changements)

================================================================================
1. VALIDATION DES IDENTIFIANTS
================================================================================

Toutes les entrées utilisateur (noms de tables, colonnes) sont validées
avant d'être utilisées dans des requêtes SQL. Cela protège contre :
    - Injections SQL (ex: "users; DROP TABLE--")
    - Caractères invalides
    - Noms trop longs

Règles appliquées :
    - Caractères autorisés : A-Z, a-z, 0-9, _
    - Longueur max : 63 caractères (limite PostgreSQL)

================================================================================
2. TRANSFORMATION DE TYPES
================================================================================

L'API AMUE renvoie des types SQLite qu'il faut convertir en PostgreSQL :

┌────────────────────┬────────────────────┬────────────────────────────────┐
│ Type SQLite         │ Type PostgreSQL    │ Notes                          │
├────────────────────┼────────────────────┼────────────────────────────────┤
│ TEXT               │ TEXT               │ Texte                          │
│ CLOB               │ TEXT               │ Texte long                     │
│ VARCHAR(50)        │ VARCHAR(50)        │ Chaîne variable                │
│ NVARCHAR(50)       │ VARCHAR(50)        │ Chaîne variable unicode        │
│ CHAR(10)           │ BPCHAR(10)         │ Chaîne fixe (blank-padded)     │
│ CHARACTER(10)      │ BPCHAR(10)         │ Chaîne fixe                    │
│ NCHAR(10)          │ BPCHAR(10)         │ Chaîne fixe unicode            │
│ INTEGER(1)         │ SMALLINT           │ Petit entier                   │
│ INTEGER(4)         │ INTEGER            │ Entier standard                │
│ INTEGER(8)         │ BIGINT             │ Grand entier                   │
│ TINYINT            │ SMALLINT           │ Petit entier                   │
│ SMALLINT           │ SMALLINT           │ Petit entier                   │
│ MEDIUMINT          │ INTEGER            │ Entier moyen                   │
│ BIGINT             │ BIGINT             │ Grand entier                   │
│ INT2               │ SMALLINT           │ Alias SMALLINT                 │
│ INT8               │ BIGINT             │ Alias BIGINT                   │
│ NUMERIC(10,2)      │ NUMERIC(10,2)      │ Nombre décimal                 │
│ DECIMAL(10,2)      │ NUMERIC(10,2)      │ Nombre décimal                 │
│ BOOLEAN            │ BOOLEAN            │ Booléen                        │
│ REAL               │ DOUBLE PRECISION   │ Réel                           │
│ DOUBLE             │ DOUBLE PRECISION   │ Réel double précision          │
│ FLOAT              │ DOUBLE PRECISION   │ Réel                           │
│ DATE               │ TIMESTAMP          │ Date+heure                     │
│ DATETIME           │ TIMESTAMP          │ Date+heure                     │
│ BLOB               │ BYTEA              │ Binaire                        │
└────────────────────┴────────────────────┴────────────────────────────────┘

================================================================================
3. FINGERPRINT (EMPREINTE DE STRUCTURE)
================================================================================

Le fingerprint est un hash MD5 qui capture la structure complète d'une table :
    - Noms et types des colonnes
    - Clés primaires

Format interne : "COLUMNS:col1:TYPE,col2:TYPE|PRIMARY_KEYS:pk1,pk2"

Utilisation :
    - Détection des changements de structure entre deux imports
    - En production : bloque l'import si structure modifiée
    - Permet de savoir si une table doit être recréée

================================================================================
USAGE
================================================================================

    from amue.utils.transformers import (
        validate_table_name,
        parse_column_definition,
        compute_structure_hash_with_pk
    )

    # Validation
    safe_name = validate_table_name('CSKS')  # 'CSKS'
    safe_col = validate_column_name('MY_COL')  # 'my_col'

    # Transformation de type
    pg_type = parse_column_definition('VARCHAR(50)')  # 'VARCHAR(50)'

    # Fingerprint
    hash = compute_structure_hash_with_pk(columns, 'id,code')

================================================================================
"""
import hashlib
import logging
import re
from typing import Any, List, Dict

logger = logging.getLogger(__name__)


# ============================================================================
# VALIDATION DES ENTRÉES (Protection injection)
# ============================================================================

def validate_table_name(table_name: str) -> str:
    """
    Valide et normalise un nom de table

    Args:
        table_name: Nom de table à valider

    Returns:
        Nom de table normalisé en majuscules

    Raises:
        ValueError: Si le nom de table est invalide

    Example:
        >>> validate_table_name('csks')
        'CSKS'
        >>> validate_table_name('DROP TABLE users--')
        ValueError: Nom de table invalide
    """
    if not table_name:
        raise ValueError("Le nom de table ne peut pas être vide")

    table_name = table_name.strip()

    if not re.match(r'^[A-Za-z0-9_]{1,63}$', table_name):
        raise ValueError(
            f"Nom de table invalide: '{table_name}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés (max 63 caractères)."
        )

    return table_name.upper()


def validate_column_name(column_name: str) -> str:
    """
    Valide et normalise un nom de colonne

    Args:
        column_name: Nom de colonne à valider

    Returns:
        Nom de colonne normalisé en minuscules

    Raises:
        ValueError: Si le nom de colonne est invalide

    Example:
        >>> validate_column_name('MY_COLUMN')
        'my_column'
    """
    if not column_name:
        raise ValueError("Le nom de colonne ne peut pas être vide")

    column_name = column_name.strip()

    if not re.match(r'^[A-Za-z0-9_]{1,63}$', column_name):
        raise ValueError(
            f"Nom de colonne invalide: '{column_name}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés (max 63 caractères)."
        )

    return column_name.lower()


def validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Valide un identifiant SQL générique (table, colonne, schéma)

    Args:
        identifier: Identifiant à valider
        identifier_type: Type d'identifiant pour le message d'erreur

    Returns:
        Identifiant normalisé

    Raises:
        ValueError: Si l'identifiant est invalide
    """
    if not identifier:
        raise ValueError(f"Le {identifier_type} ne peut pas être vide")

    identifier = identifier.strip()

    if not re.match(r'^[A-Za-z0-9_]{1,63}$', identifier):
        raise ValueError(
            f"{identifier_type.capitalize()} invalide: '{identifier}'. "
            "Seuls les caractères alphanumériques et underscores sont autorisés."
        )

    return identifier


# ============================================================================
# TRANSFORMATION DE TYPES
# ============================================================================


def parse_column_definition(definition: str) -> str:
    """
    Convertit une définition de colonne SQLite en type PostgreSQL

    Args:
        definition: Type de colonne SQLite (ex: 'VARCHAR(50)', 'NUMERIC(10,2)', 'CHAR(10)')

    Returns:
        Type PostgreSQL équivalent (ex: 'VARCHAR(50)', 'NUMERIC(10,2)', 'BPCHAR(10)')

    Examples:
        >>> parse_column_definition('VARCHAR(50)')
        'VARCHAR(50)'
        >>> parse_column_definition('NUMERIC(10,2)')
        'NUMERIC(10,2)'
        >>> parse_column_definition('INTEGER(1)')
        'SMALLINT'
        >>> parse_column_definition('DATE')
        'TIMESTAMP'
    """
    definition = definition.strip()

    # Mapping des types SQLite vers PostgreSQL
    type_mapping = {
        # Texte
        'TEXT': 'TEXT',
        'CLOB': 'TEXT',
        'CHAR': 'BPCHAR',
        'CHARACTER': 'BPCHAR',
        'VARCHAR': 'VARCHAR',
        'NCHAR': 'BPCHAR',
        'NVARCHAR': 'VARCHAR',
        # Entiers
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'TINYINT': 'SMALLINT',
        'SMALLINT': 'SMALLINT',
        'MEDIUMINT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'INT2': 'SMALLINT',
        'INT8': 'BIGINT',
        # Numériques
        'NUMERIC': 'NUMERIC',
        'DECIMAL': 'NUMERIC',
        'BOOLEAN': 'BOOLEAN',
        # Réels
        'REAL': 'DOUBLE PRECISION',
        'DOUBLE': 'DOUBLE PRECISION',
        'FLOAT': 'DOUBLE PRECISION',
        # Dates
        'DATE': 'TIMESTAMP',
        'DATETIME': 'TIMESTAMP',
        # Binaires
        'BLOB': 'BYTEA',
    }

    # Parse le type et ses paramètres (ex: VARCHAR2(50) -> VARCHAR2 + (50))
    match = re.match(r'(\w+)(\(.*?\))?', definition, re.IGNORECASE)

    if not match:
        logger.warning(f"[WARN] Type invalide '{definition}', utilisation de TEXT par defaut")
        return 'TEXT'

    base_type = match.group(1).upper()
    params = match.group(2) or ''

    # Cas spécial : INTEGER avec paramètre -> adapter la taille
    if base_type in ('INTEGER', 'INT') and params:
        try:
            size = int(params.strip('()'))
            if size <= 2:
                return 'SMALLINT'
            elif size <= 4:
                return 'INTEGER'
            else:
                return 'BIGINT'
        except (ValueError, AttributeError):
            return 'INTEGER'

    # Convertit le type de base
    pg_type = type_mapping.get(base_type, base_type)

    # Types sans paramètres en PostgreSQL
    types_without_params = (
        'INTEGER', 'SMALLINT', 'BIGINT',
        'TIMESTAMP', 'TEXT', 'DOUBLE PRECISION',
        'BYTEA', 'BOOLEAN'
    )

    if pg_type in types_without_params and params:
        # Enlève les paramètres pour ces types
        return pg_type

    # Retourne le type avec ses paramètres
    return pg_type + params


# ============================================================================
# FINGERPRINT / HASH
# ============================================================================

def compute_structure_hash_with_pk(columns: List[Dict[str, str]], primary_keys: str = '') -> str:
    """
    Calcule un hash MD5 de la structure d'une table incluant les clés primaires

    Le fingerprint inclut maintenant :
    - Les colonnes avec leurs types
    - Les clés primaires (ordre important)

    Cela permet de détecter les changements de clés primaires en plus des changements
    de colonnes/types.

    Args:
        columns: Liste de dicts avec clés 'name' et 'type_postgres'
        primary_keys: Chaîne avec clés primaires séparées par virgules (ex: "id,code")

    Returns:
        Hash MD5 hexadécimal (32 caractères)

    Examples:
        >>> columns = [
        ...     {'name': 'id', 'type_postgres': 'INTEGER'},
        ...     {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
        ... ]
        >>> compute_structure_hash_with_pk(columns, 'id')
        'b2c3d4e5f6a7...'
        >>> compute_structure_hash_with_pk(columns, 'id,name')
        'c3d4e5f6a7b8...'  # Hash différent car PK différente
    """
    # Partie 1 : Structure des colonnes
    columns_str = ','.join([
        f"{col['name']}:{col['type_postgres']}"
        for col in columns
    ])

    # Partie 2 : Clés primaires normalisées
    if primary_keys:
        # Normalise les clés primaires (trim, lowercase, ordre)
        pk_list = [pk.strip().lower() for pk in primary_keys.split(',') if pk.strip()]
        pk_list.sort()  # Tri pour garantir la cohérence
        pk_str = ','.join(pk_list)
    else:
        pk_str = 'NO_PRIMARY_KEY'

    # Combinaison des deux parties
    full_structure = f"COLUMNS:{columns_str}|PRIMARY_KEYS:{pk_str}"

    # Calcule le hash MD5
    fingerprint = hashlib.md5(full_structure.encode('utf-8')).hexdigest()

    logger.info(f"[FINGERPRINT] Colonnes: {len(columns)}, PK: {pk_str}")
    logger.info(f"[FINGERPRINT] Hash: {fingerprint}")

    return fingerprint


def format_primary_keys(primary_keys: str) -> List[str]:
    """
    Parse et nettoie une chaîne de clés primaires

    Args:
        primary_keys: Chaîne avec clés séparées par virgules (ex: "key1, key2, key3")

    Returns:
        Liste des clés nettoyées en lowercase

    Example:
        >>> format_primary_keys("ID, Name, Date")
        ['id', 'name', 'date']
        >>> format_primary_keys("")
        []
    """
    if not primary_keys:
        return []

    return [
        pk.strip().lower()
        for pk in primary_keys.split(',')
        if pk.strip()
    ]


def compare_fingerprints(old_fingerprint: str, new_fingerprint: str,
                         table_name: str = None) -> Dict[str, Any]:
    """
    Compare deux fingerprints et retourne des informations détaillées

    Utile pour diagnostiquer quel aspect de la structure a changé

    Args:
        old_fingerprint: Ancien hash
        new_fingerprint: Nouveau hash
        table_name: Nom de la table (pour logs)

    Returns:
        Dict avec 'changed' (bool), 'old', 'new', 'table_name'

    Example:
        >>> compare_fingerprints('abc123', 'def456', 'CSKS')
        {'changed': True, 'old': 'abc123', 'new': 'def456', 'table_name': 'CSKS'}
    """
    changed = old_fingerprint != new_fingerprint

    result = {
        'changed': changed,
        'old': old_fingerprint,
        'new': new_fingerprint,
        'table_name': table_name
    }

    if changed and table_name:
        logger.info(f"[FINGERPRINT] {table_name}: Structure changée")
        logger.info(f"  Ancien: {old_fingerprint[:16]}...")
        logger.info(f"  Nouveau: {new_fingerprint[:16]}...")

    return result
