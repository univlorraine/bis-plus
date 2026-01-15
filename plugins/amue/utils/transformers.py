"""
Fonctions de transformation pour les données AMUE
Mise à jour avec fingerprint incluant les clés primaires
"""
import re
import hashlib
from typing import List, Dict
from amue.utils.logger import get_logger

logger = get_logger(__name__)


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
        'TIMESTAMP', 'TEXT', 'DOUBLE PRECISION'
    )

    if pg_type in types_without_params and params:
        # Enlève les paramètres pour ces types
        return pg_type

    # Retourne le type avec ses paramètres
    return pg_type + params


# def compute_structure_hash(columns: List[Dict[str, str]]) -> str:
#     """
#     Calcule un hash MD5 de la structure d'une table (colonnes uniquement)
#
#     ⚠️ DEPRECATED: Utiliser compute_structure_hash_with_pk() à la place
#     Cette fonction est conservée pour compatibilité mais ne devrait plus être utilisée
#
#     Args:
#         columns: Liste de dicts avec clés 'name' et 'type_postgres'
#
#     Returns:
#         Hash MD5 hexadécimal (32 caractères)
#
#     Example:
#         >>> columns = [
#         ...     {'name': 'id', 'type_postgres': 'INTEGER'},
#         ...     {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
#         ... ]
#         >>> compute_structure_hash(columns)
#         'a1b2c3d4e5f6...'
#     """
#     # Crée une chaîne normalisée : "col1 : type1, col2 : type2, ..."
#     structure_str = ','.join([
#         f"{col['name']}:{col['type_postgres']}"
#         for col in columns
#     ])
#
#     # Calcule le hash MD5
#     return hashlib.md5(structure_str.encode('utf-8')).hexdigest()


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
                        table_name: str = None) -> Dict[str, any]:
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