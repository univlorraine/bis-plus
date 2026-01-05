"""
Fonctions de transformation pour les données AMUE
"""
import re
import hashlib
from typing import List, Dict


def parse_column_definition(definition: str) -> str:
    """
    Convertit une définition de colonne AMUE/Oracle en type PostgreSQL

    Args:
        definition: Type de colonne AMUE (ex: 'VARCHAR2(50)', 'NUMBER(10,2)', 'CHAR(10)')

    Returns:
        Type PostgreSQL équivalent (ex: 'VARCHAR(50)', 'NUMERIC(10,2)', 'BPCHAR(10)')

    Examples:
        >>> parse_column_definition('VARCHAR2(50)')
        'VARCHAR(50)'
        >>> parse_column_definition('NUMBER(10,2)')
        'NUMERIC(10,2)'
        >>> parse_column_definition('INTEGER(1)')
        'SMALLINT'
        >>> parse_column_definition('DATE')
        'TIMESTAMP'
    """
    definition = definition.strip()

    # Mapping des types AMUE/Oracle vers PostgreSQL
    type_mapping = {
        'VARCHAR2': 'VARCHAR',
        'NUMBER': 'NUMERIC',
        'DATE': 'TIMESTAMP',
        'CLOB': 'TEXT',
        'BLOB': 'BYTEA',
        'CHAR': 'BPCHAR',
        'VARCHAR': 'VARCHAR',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'FLOAT': 'DOUBLE PRECISION',
        'DECIMAL': 'NUMERIC',
        'SMALLINT': 'SMALLINT',
        'BIGINT': 'BIGINT',
        'DEC': 'NUMERIC',
    }

    # Parse le type et ses paramètres (ex: VARCHAR2(50) -> VARCHAR2 + (50))
    match = re.match(r'(\w+)(\(.*?\))?', definition, re.IGNORECASE)

    if not match:
        print(f"[WARN] Type invalide '{definition}', utilisation de TEXT par defaut")
        return 'TEXT'

    base_type = match.group(1).upper()
    params = match.group(2) or ''

    # Cas spécial: INTEGER avec paramètre -> adapter la taille
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
        'TIMESTAMP', 'TEXT', 'DOUBLE PRECISION'
    )

    if pg_type in types_without_params and params:
        # Enlève les paramètres pour ces types
        return pg_type

    # Retourne le type avec ses paramètres
    return pg_type + params


def compute_structure_hash(columns: List[Dict[str, str]]) -> str:
    """
    Calcule un hash MD5 de la structure d'une table

    Utilisé pour détecter les changements de structure entre deux versions

    Args:
        columns: Liste de dicts avec clés 'name' et 'type_postgres'

    Returns:
        Hash MD5 hexadécimal (32 caractères)

    Example:
        >>> columns = [
        ...     {'name': 'id', 'type_postgres': 'INTEGER'},
        ...     {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
        ... ]
        >>> compute_structure_hash(columns)
        'a1b2c3d4e5f6...'
    """
    # Crée une chaîne normalisée: "col1:type1,col2:type2,..."
    structure_str = ','.join([
        f"{col['name']}:{col['type_postgres']}"
        for col in columns
    ])

    # Calcule le hash MD5
    return hashlib.md5(structure_str.encode('utf-8')).hexdigest()


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