"""
Conversion des types SQLite (API AMUE) vers PostgreSQL.

Ce module reste AMUE-spécifique car le mapping est piloté par la variable
Airflow `TYPE_MAPPING_SQLITE_TO_POSTGRES` propre à l'import AMUE.

Les utilitaires génériques (validation d'identifiants, fingerprint) ont été
déplacés dans `common.utils.validators` et `common.utils.fingerprint`.
"""
import json
import logging
import re
import threading

from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


_type_mapping_cache = None
_type_mapping_lock = threading.Lock()


def _get_type_mapping() -> dict:
    """Charge le mapping SQLite → PostgreSQL depuis la variable Airflow (cache thread-safe)."""
    global _type_mapping_cache
    if _type_mapping_cache is not None:
        return _type_mapping_cache
    with _type_mapping_lock:
        if _type_mapping_cache is not None:
            return _type_mapping_cache
        raw = VarMgr.get('TYPE_MAPPING_SQLITE_TO_POSTGRES')
        mapping = json.loads(raw) if isinstance(raw, str) else raw
        _type_mapping_cache = {k.upper(): v for k, v in mapping.items()}
        logger.info(f"[TRANSFORMERS] Type mapping charge: {len(_type_mapping_cache)} types")
    return _type_mapping_cache


def parse_column_definition(definition: str) -> str:
    """
    Convertit une définition de colonne SQLite en type PostgreSQL.

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
    type_mapping = _get_type_mapping()
    match = re.match(r'(\w+)(\(.*?\))?', definition, re.IGNORECASE)
    if not match:
        logger.warning(f"[WARN] Type invalide '{definition}', utilisation de TEXT par defaut")
        return 'TEXT'

    base_type = match.group(1).upper()
    params = match.group(2) or ''

    # Cas spécial : INTEGER avec paramètre → adapter la taille
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

    pg_type = type_mapping.get(base_type, base_type)

    types_without_params = (
        'INTEGER', 'SMALLINT', 'BIGINT',
        'TIMESTAMP', 'TEXT', 'DOUBLE PRECISION',
        'BYTEA', 'BOOLEAN',
    )
    if pg_type in types_without_params and params:
        return pg_type

    return pg_type + params
