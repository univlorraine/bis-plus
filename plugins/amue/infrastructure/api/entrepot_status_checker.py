"""
Layer: infrastructure

Vérificateur de statut pour l'API AMUE Entrepôt v1.2 (PostgREST).

Même interface que AMUEStatusChecker mais consomme l'endpoint
`/rpc/get_status` de la nouvelle API au lieu de l'admin CDV.
"""
import logging
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class EntrepotStatusChecker:
    """
    Vérifie le statut de l'API AMUE Entrepôt via /rpc/get_status.

    Interface identique à AMUEStatusChecker pour une substitution transparente.
    """

    def __init__(self, api_hook):
        self.api_hook = api_hook
        univ = VarMgr.get_required('universite')
        raw = VarMgr.get_required('api_endpoint_entrepot')
        try:
            self.base_endpoint = Template(raw).substitute(univ=univ).rstrip('/')
        except KeyError as exc:
            raise AirflowException(
                f"Placeholder inconnu dans api_endpoint_entrepot : {exc}"
            ) from exc

    def get_current_status(self) -> Dict:
        """
        Retourne le statut courant par table.

        Returns:
            Dict[str, Dict] keyed by uppercase table name.
        """
        logger.info("[STATUS-ENTREPOT] Récupération statut courant")
        response = self.api_hook.call_api(f"{self.base_endpoint}/rpc/get_status")

        if not isinstance(response, dict) or 'status' not in response:
            raise ValueError(
                f"Format réponse invalide depuis /rpc/get_status: {type(response)}"
            )

        tables_status = self._parse_tables_status(response.get('status', []))
        logger.info(f"[STATUS-ENTREPOT] {len(tables_status)} tables trouvées")
        return tables_status

    def fetch_full_status(self) -> Dict:
        """
        Retourne le statut complet (http_status, finish, start, tables_status).

        Même structure de retour que AMUEStatusChecker.fetch_full_status().
        """
        logger.info("[STATUS-ENTREPOT] Récupération statut complet")
        try:
            response = self.api_hook.call_api(f"{self.base_endpoint}/rpc/get_status")

            if not isinstance(response, dict):
                logger.warning("[STATUS-ENTREPOT] Réponse non-JSON")
                return {
                    'http_status': 200,
                    'finish': None,
                    'tables_status': {},
                    'raw_response': response,
                }

            finish_value = response.get('finish')
            tables_status = self._parse_tables_status(response.get('status', []))

            logger.info(
                f"[STATUS-ENTREPOT] HTTP 200, finish={finish_value or 'non renseigné'}, "
                f"{len(tables_status)} tables"
            )

            return {
                'http_status': 200,
                'finish': finish_value,
                'start': response.get('start'),
                'tables_status': tables_status,
                'raw_response': response,
            }

        except Exception as e:
            http_status = getattr(getattr(e, 'response', None), 'status_code', 500)
            logger.warning(f"[STATUS-ENTREPOT] Erreur HTTP {http_status}: {e}")
            return {
                'http_status': http_status,
                'finish': None,
                'tables_status': {},
                'raw_response': None,
                'error': str(e),
            }

    def _parse_tables_status(self, status_list: List) -> Dict:
        """Parse la liste de statuts en dict keyed by uppercase table name."""
        tables_status = {}
        if not isinstance(status_list, list):
            return tables_status

        for table_info in status_list:
            if not isinstance(table_info, dict):
                continue
            table_name = table_info.get('name', '').upper()
            if not table_name:
                continue
            tables_status[table_name] = {
                'status': table_info.get('status', 'UNKNOWN'),
                'mode': table_info.get('mode', 'UNKNOWN'),
                'count': table_info.get('count', 0),
                'row_size': table_info.get('row_size', 0),
                'finish': table_info.get('finish', None),
            }

        return tables_status
