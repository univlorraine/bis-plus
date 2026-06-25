"""
Layer: infrastructure

Streaming de données depuis l'API AMUE Entrepôt v1.2 (PostgREST).

Différences avec AMUEDataStreamer (API CDV) :
  - Réponse directe : tableau JSON (pas de wrapper data.row)
  - Pagination via /rpc/info_pagination (pas de count dans la réponse data)
  - Filtre delta : `col=gte.YYYYMMDD` (params PostgREST, pas `q=COL>='date'`)
  - Paramètre `order` obligatoire pour éviter les doublons entre pages
"""
import logging
from datetime import datetime
from typing import Any, Dict, Generator, Optional

from airflow.exceptions import AirflowException
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 5000


class EntrepotDataStreamer:
    """
    Streamer pour l'API AMUE Entrepôt v1.2.

    Interface identique à AMUEDataStreamer.stream_data() pour substitution transparente.
    """

    def __init__(self, api_hook, base_endpoint: str):
        self.api_hook = api_hook
        self.base_endpoint = base_endpoint.rstrip('/')

    def stream_data(
        self,
        table_name: str,
        import_config: Dict[str, Any],
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Récupère les données en streaming (générateur).

        Args:
            table_name: Nom de la table (maj ou min, normalisé en minuscules)
            import_config: Config d'import (import_type, delta, last_import, primary_key)

        Yields:
            Dict représentant une ligne de données.
        """
        table_lower = table_name.lower()
        import_type = import_config.get('import_type', 'full')
        delta_column = import_config.get('delta', '')
        last_import = import_config.get('last_import', '')

        delta_date: Optional[str] = None
        if import_type == 'delta' and delta_column and last_import:
            delta_date = self._format_date(last_import)
            logger.info(f"[ENTREPOT] Delta sur {delta_column} >= {delta_date}")

        top = self._get_page_size(table_name, delta_column, delta_date)
        order = self._build_order(import_config)

        skip = 0
        page = 1

        while True:
            params: Dict[str, Any] = {
                'limit': top,
                'offset': skip,
                'order': order,
            }
            if delta_date and delta_column:
                params[delta_column.lower()] = f'gte.{delta_date}'

            endpoint = f"{self.base_endpoint}/{table_lower}"
            logger.info(f"[ENTREPOT] Page {page} (offset={skip})")

            try:
                response = self.api_hook.call_api(endpoint, params)
            except Exception as e:
                raise AirflowException(
                    f"[ENTREPOT] Erreur lors de la récupération de {table_name} page {page}: {e}"
                ) from e

            if not isinstance(response, list):
                raise AirflowException(
                    f"[ENTREPOT] Format réponse inattendu pour {table_name}: {type(response)}"
                )

            rows = response
            if not rows:
                break

            logger.info(f"[ENTREPOT] {len(rows)} lignes récupérées (page {page})")

            for row in rows:
                yield row

            if len(rows) < top:
                break

            skip += len(rows)
            page += 1

    def _get_page_size(
        self,
        table_name: str,
        delta_column: str,
        delta_date: Optional[str],
    ) -> int:
        """Interroge /rpc/info_pagination pour obtenir la taille de page optimale."""
        params: Dict[str, Any] = {'nom_table': table_name.upper()}
        if delta_date and delta_column:
            params['w'] = f"{delta_column}>= '{delta_date}'"

        try:
            response = self.api_hook.call_api(
                f"{self.base_endpoint}/rpc/info_pagination", params
            )
            if isinstance(response, dict):
                top = int(response.get('top', 0) or 0)
                if top > 0:
                    logger.info(f"[ENTREPOT] Page size depuis info_pagination: {top}")
                    return top
            logger.warning(
                f"[ENTREPOT] info_pagination réponse inattendue ({response}), "
                f"fallback sur amue_import_batch_size"
            )
        except Exception as e:
            logger.warning(
                f"[ENTREPOT] info_pagination indisponible ({e}), "
                f"fallback sur amue_import_batch_size"
            )

        return VarMgr.get_int('amue_import_batch_size', default=_DEFAULT_BATCH_SIZE, min_value=1)

    @staticmethod
    def _build_order(import_config: Dict[str, Any]) -> str:
        """Construit le paramètre `order` depuis les clés primaires de la config."""
        pk_str = import_config.get('primary_key', '')
        if not pk_str:
            return 'ctid'  # fallback minimal pour éviter les doublons
        return ','.join(k.strip().lower() for k in pk_str.split(',') if k.strip())

    @staticmethod
    def _format_date(date_str: str) -> str:
        """Formate une date ISO en YYYYMMDD pour le filtre delta PostgREST."""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y%m%d')
        except (ValueError, AttributeError) as e:
            logger.warning(f"[ENTREPOT] Format de date invalide '{date_str}': {e}")
            return date_str.replace('-', '')[:8]
