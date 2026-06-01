"""
Calcul et comparaison de fingerprints de structure de tables.

Module générique partagé AMUE / ECC. Le fingerprint sert à détecter les
changements de structure (colonnes, types, clés primaires) entre deux
exécutions.
"""
import hashlib
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def compute_structure_hash_with_pk(
    columns: List[Dict[str, str]],
    primary_keys: str = '',
    type_key: str = 'type_postgres',
) -> str:
    """
    Calcule un hash SHA-256 de la structure d'une table (colonnes + clés primaires).

    Args:
        columns: Liste de dicts avec clés 'name' et le type spécifié par type_key.
        primary_keys: Chaîne CSV des clés primaires (ordre normalisé pour stabilité).
        type_key: Clé du dict à utiliser pour le type (ex: 'type_postgres', 'type_original').

    Returns:
        Hash SHA-256 hexadécimal (64 caractères).
    """
    columns_str = ','.join(f"{col['name']}:{col[type_key]}" for col in columns)
    if primary_keys:
        pk_list = sorted(pk.strip().lower() for pk in primary_keys.split(',') if pk.strip())
        pk_str = ','.join(pk_list)
    else:
        pk_str = 'NO_PRIMARY_KEY'
    full_structure = f"COLUMNS:{columns_str}|PRIMARY_KEYS:{pk_str}"
    fingerprint = hashlib.sha256(full_structure.encode('utf-8')).hexdigest()
    logger.info(f"[FINGERPRINT] Colonnes: {len(columns)}, PK: {pk_str}")
    logger.info(f"[FINGERPRINT] Hash: {fingerprint}")
    return fingerprint


def format_primary_keys(primary_keys: str) -> List[str]:
    """
    Parse une chaîne CSV de clés primaires en liste lowercase nettoyée.

    Example:
        >>> format_primary_keys("ID, Name, Date")
        ['id', 'name', 'date']
    """
    if not primary_keys:
        return []
    return [pk.strip().lower() for pk in primary_keys.split(',') if pk.strip()]


def compare_fingerprints(
    old_fingerprint: str,
    new_fingerprint: str,
    table_name: str = None,
) -> Dict[str, Any]:
    """
    Compare deux fingerprints et retourne un dict descriptif.

    Returns:
        {'changed': bool, 'old': str, 'new': str, 'table_name': str | None}
    """
    changed = old_fingerprint != new_fingerprint
    result = {
        'changed': changed,
        'old': old_fingerprint,
        'new': new_fingerprint,
        'table_name': table_name,
    }
    if changed and table_name:
        logger.info(f"[FINGERPRINT] {table_name}: Structure changée")
        logger.info(f"  Ancien: {old_fingerprint[:16]}...")
        logger.info(f"  Nouveau: {new_fingerprint[:16]}...")
    return result
