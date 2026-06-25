"""
Layer: domain

Récupération de la structure d'une table depuis l'API AMUE.

Encapsule les appels à l'API AMUE pour obtenir :
    - la définition des colonnes (`{table}.def`) avec mapping vers PostgreSQL
    - la liste des clés primaires (`{table}.keys`)
"""
import logging
from typing import Dict, List

from amue.domain.transformers import parse_column_definition

logger = logging.getLogger(__name__)

_META_COLUMNS = {'_SOURCE', '_IMPORTED_AT'}


def split_column_defs(columns_def: str) -> list:
    """
    Découpe une chaîne de définitions de colonnes en respectant les parenthèses.

    Un split naïf sur ',' casse les types avec paramètres comme NUMERIC(15,2).
    Cette fonction ne coupe que les virgules au niveau 0 de parenthèses.

    Example:
        "MANDT CHAR(3),WKGBTR NUMERIC(15,2),CODE CHAR(4)"
        → ["MANDT CHAR(3)", "WKGBTR NUMERIC(15,2)", "CODE CHAR(4)"]
    """
    parts = []
    depth = 0
    current = []
    for char in columns_def:
        if char == '(':
            depth += 1
            current.append(char)
        elif char == ')':
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(char)
    if current:
        parts.append(''.join(current))
    return parts


class APIStructureFetcher:
    """Récupère structure et clés primaires depuis l'API AMUE."""

    def __init__(self, api_hook, endpoint: str):
        self.api_hook = api_hook
        self.endpoint = endpoint

    def fetch_structure(self, table_name: str) -> List[Dict]:
        """
        Récupère la définition des colonnes depuis l'API AMUE.

        Returns:
            Liste de dicts {'name', 'type_original', 'type_postgres'}.
        """
        params = {'get': f'{table_name.upper()}.def', 'f': 'json'}
        structure_response = self.api_hook.call_api(self.endpoint, params)

        if isinstance(structure_response, str):
            columns_def = structure_response.strip()
        elif isinstance(structure_response, dict):
            columns_def = structure_response.get('definition') or str(structure_response)
        else:
            columns_def = str(structure_response)

        columns = []
        for col_def in split_column_defs(columns_def):
            col_def = col_def.strip()
            if not col_def:
                continue
            parts = col_def.split(None, 1)
            if len(parts) >= 2:
                col_name = parts[0].strip().upper()
                if col_name in _META_COLUMNS:
                    continue
                col_type = parts[1].strip().upper()
                pg_type = parse_column_definition(col_type)
                columns.append({
                    'name': col_name,
                    'type_original': col_type,
                    'type_postgres': pg_type,
                })

        if not columns:
            raise ValueError("Aucune colonne trouvée")
        return columns

    def fetch_primary_keys(self, table_name: str) -> str:
        """
        Récupère les clés primaires depuis l'API AMUE (CSV).

        Returns:
            Chaîne CSV des clés primaires, ou '' en cas d'erreur ou de réponse vide.
        """
        logger.info(f"[STRUCTURE_CHECK] Appel API pour clés primaires de {table_name}")
        params = {'get': f'{table_name.upper()}.keys', 'f': 'json'}
        try:
            keys_response = self.api_hook.call_api(self.endpoint, params)
            if isinstance(keys_response, str):
                result = keys_response.strip()
            elif isinstance(keys_response, list):
                result = ','.join(str(k) for k in keys_response if k)
            elif isinstance(keys_response, dict):
                result = ','.join(str(k) for k in keys_response.get('keys', []) if k)
            else:
                logger.warning(f"[WARN] Format de réponse inattendu pour les clés: {type(keys_response)}")
                result = ''

            if result:
                logger.info(f"[STRUCTURE_CHECK] Clés trouvées: {result}")
            else:
                logger.warning("[WARN] Aucune clé primaire retournée par l'API")
            return result
        except Exception as e:
            logger.error(f"[ERROR] Erreur lors de la récupération des clés: {str(e)}")
            return ''
