"""
Layer: domain

Comparaison de fingerprints et diff de structure entre API et PostgreSQL.

Fonctions pures (sans I/O) : prennent en entrée les colonnes des deux côtés
et produisent un diff lisible / un dict des changements détectés.
"""
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def check_fingerprint_changes(
    table_name: str,
    new_fp_api: str,
    old_fp_api: str,
    new_fp_local: str,
    old_fp_local: str,
    exists: bool,
) -> Dict[str, bool]:
    """
    Vérifie quels fingerprints ont changé.

    Returns:
        {'api_changed': bool, 'ul_changed': bool}
    """
    if not exists:
        return {'api_changed': False, 'ul_changed': False}

    api_changed = bool(old_fp_api and new_fp_api and old_fp_api != new_fp_api)
    ul_changed = bool(old_fp_local and new_fp_local and old_fp_local != new_fp_local)

    if api_changed:
        logger.info(f"[STRUCTURE_CHECK] {table_name}: fingerprint_API changé")
        logger.info(f"  Ancien: {old_fp_api[:16]}...")
        logger.info(f"  Nouveau: {new_fp_api[:16]}...")
    if ul_changed:
        logger.info(f"[STRUCTURE_CHECK] {table_name}: fingerprint_local changé")
        logger.info(f"  Ancien: {old_fp_local[:16]}...")
        logger.info(f"  Nouveau: {new_fp_local[:16]}...")

    return {'api_changed': api_changed, 'ul_changed': ul_changed}


def format_pg_type(data_type: str, char_len, num_prec, num_scale) -> str:
    """
    Reconstruit un type PG lisible depuis les champs information_schema.

    Returns:
        Type formaté (ex: 'VARCHAR(50)', 'NUMERIC(10,2)', 'TIMESTAMP')
    """
    dt = data_type.upper()

    if dt == 'CHARACTER VARYING':
        return f"VARCHAR({char_len})" if char_len else "VARCHAR"
    if dt == 'CHARACTER':
        return f"BPCHAR({char_len})" if char_len else "BPCHAR"
    if dt == 'NUMERIC':
        if num_prec is not None and num_scale is not None:
            return f"NUMERIC({num_prec},{num_scale})"
        if num_prec is not None:
            return f"NUMERIC({num_prec})"
        return "NUMERIC"
    if dt == 'TIMESTAMP WITHOUT TIME ZONE':
        return "TIMESTAMP"
    if dt == 'TIMESTAMP WITH TIME ZONE':
        return "TIMESTAMPTZ"
    if dt == 'DOUBLE PRECISION':
        return "DOUBLE PRECISION"

    return dt


def compute_structure_diff(
    existing_columns: List[Dict],
    new_columns: List[Dict],
) -> str:
    """
    Compare colonnes existantes PG vs nouvelles colonnes API → diff lisible.

    Symboles :
        + col_name (TYPE)           colonne ajoutée
        - col_name (TYPE)           colonne supprimée
        ~ col_name: OLD -> NEW      changement de type
    """
    existing_map = {col['name']: col['type_postgres'] for col in existing_columns}
    new_map = {col['name']: col['type_postgres'] for col in new_columns}

    diff_lines = []
    for name in new_map:
        if name not in existing_map:
            diff_lines.append(f"  + {name} ({new_map[name]})")
    for name in existing_map:
        if name not in new_map:
            diff_lines.append(f"  - {name} ({existing_map[name]})")
    for name in new_map:
        if name in existing_map and new_map[name] != existing_map[name]:
            diff_lines.append(f"  ~ {name}: {existing_map[name]} -> {new_map[name]}")

    if diff_lines:
        return "Differences:\n" + "\n".join(diff_lines)
    return "Aucune difference de colonnes detectee; le changement provient probablement des cles primaires."
