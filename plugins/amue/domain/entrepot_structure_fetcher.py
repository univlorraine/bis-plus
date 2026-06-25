"""
Layer: domain

Récupération de la structure d'une table depuis l'API AMUE Entrepôt v1.2.

Différences avec APIStructureFetcher (CDV) :
  - Endpoint : /rpc/get_file?nom_table=TABLE.def (pas ?get=TABLE.def&f=json)
  - Header Accept: text/plain obligatoire
  - Réponse texte brut (idem CDV après parsing)
"""
import logging
from typing import Dict, List

from amue.domain.structure_fetcher import split_column_defs
from amue.domain.transformers import parse_column_definition

logger = logging.getLogger(__name__)

_META_COLUMNS = {'_SOURCE', '_IMPORTED_AT'}
_PLAIN_HEADERS = {'Accept': 'text/plain'}


class EntrepotStructureFetcher:
    """
    Récupère structure et clés primaires depuis l'API AMUE Entrepôt.

    Interface identique à APIStructureFetcher pour substitution transparente.
    """

    def __init__(self, api_hook, base_endpoint: str):
        self.api_hook = api_hook
        self.base_endpoint = base_endpoint.rstrip('/')

    def fetch_structure(self, table_name: str) -> List[Dict]:
        """
        Récupère la définition des colonnes via /rpc/get_file?nom_table=TABLE.def.

        Returns:
            Liste de dicts {'name', 'type_original', 'type_postgres'}.
        """
        response = self.api_hook.call_api(
            f"{self.base_endpoint}/rpc/get_file",
            params={'nom_table': f'{table_name.upper()}.def'},
            extra_headers=_PLAIN_HEADERS,
        )

        if isinstance(response, str):
            columns_def = response.strip()
        elif isinstance(response, dict):
            columns_def = response.get('definition') or str(response)
        else:
            columns_def = str(response)

        columns = []
        for col_def in split_column_defs(columns_def):
            col_def = col_def.strip()
            if not col_def:
                continue
            parts = col_def.split(None, 1)
            if len(parts) < 2:
                continue
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
            raise ValueError(f"Aucune colonne trouvée pour {table_name}")
        return columns

    def fetch_primary_keys(self, table_name: str) -> str:
        """
        Récupère les clés primaires via /rpc/get_file?nom_table=TABLE.keys.

        Returns:
            Chaîne CSV des clés primaires, ou '' en cas d'erreur.
        """
        logger.info(f"[ENTREPOT-STRUCT] Récupération PKs pour {table_name}")
        try:
            response = self.api_hook.call_api(
                f"{self.base_endpoint}/rpc/get_file",
                params={'nom_table': f'{table_name.upper()}.keys'},
                extra_headers=_PLAIN_HEADERS,
            )

            if isinstance(response, str):
                result = response.strip()
            elif isinstance(response, list):
                result = ','.join(str(k) for k in response if k)
            elif isinstance(response, dict):
                result = ','.join(str(k) for k in response.get('keys', []) if k)
            else:
                logger.warning(
                    f"[ENTREPOT-STRUCT] Format inattendu pour PKs de {table_name}: {type(response)}"
                )
                result = ''

            if result:
                logger.info(f"[ENTREPOT-STRUCT] PKs trouvées: {result}")
            else:
                logger.warning(f"[ENTREPOT-STRUCT] Aucune PK retournée pour {table_name}")
            return result

        except Exception as e:
            logger.error(f"[ENTREPOT-STRUCT] Erreur PKs {table_name}: {e}")
            return ''
