"""
Layer: application

Factory d'API AMUE — sélection de l'implémentation via `amue_api_source`.

Architecture : registre de sources + dispatch générique.

Pour ajouter une nouvelle source (ex. 'v2') :
  1. Créer les classes StatusChecker, DataStreamer, StructureFetcher correspondantes
  2. Ajouter une entrée dans _SOURCE_REGISTRY
  3. Ajouter la variable d'endpoint dans config/airflow_variables.json

Variables Airflow lues :
  amue_api_source       : identifiant de la source active  (défaut : 'cdv')
  api_endpoint_table    : endpoint données CDV (avec ${univ})
  api_endpoint_admin    : endpoint structure CDV (avec ${univ})
  api_endpoint_entrepot : endpoint de base pour 'entrepot' (avec ${univ})
"""
import logging
from string import Template

from airflow.exceptions import AirflowException
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)

_DEFAULT_SOURCE = 'cdv'


def _get_source() -> str:
    return VarMgr.get('amue_api_source', default=_DEFAULT_SOURCE)


def _resolve_endpoint(var_name: str) -> str:
    """Lit une variable d'endpoint et substitue ${univ}."""
    univ = VarMgr.get_required('universite')
    raw = VarMgr.get_required(var_name)
    try:
        return Template(raw).substitute(univ=univ).rstrip('/')
    except KeyError as exc:
        raise AirflowException(
            f"Placeholder inconnu dans la variable '{var_name}' : {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Registre des sources
#
# Chaque entrée est un dict de trois callables :
#   status(api_hook)  -> instance de StatusChecker
#   streamer(api_hook) -> instance de DataStreamer
#   fetcher(api_hook)  -> instance de StructureFetcher
#
# Chaque callable est responsable de résoudre son propre endpoint via
# _resolve_endpoint(), ce qui centralise toute la logique d'endpoint ici.
# ---------------------------------------------------------------------------

def _build_registry() -> dict:
    def _cdv_status(api_hook):
        from amue.infrastructure.api.status_checker import AMUEStatusChecker
        return AMUEStatusChecker(api_hook)

    def _cdv_streamer(api_hook):
        from amue.infrastructure.api.data_streamer import AMUEDataStreamer
        return AMUEDataStreamer(api_hook, _resolve_endpoint('api_endpoint_table'))

    def _cdv_fetcher(api_hook):
        from amue.domain.structure_fetcher import APIStructureFetcher
        return APIStructureFetcher(api_hook, _resolve_endpoint('api_endpoint_admin'))

    def _entrepot_status(api_hook):
        from amue.infrastructure.api.entrepot_status_checker import EntrepotStatusChecker
        return EntrepotStatusChecker(api_hook)

    def _entrepot_streamer(api_hook):
        from amue.infrastructure.api.entrepot_data_streamer import EntrepotDataStreamer
        return EntrepotDataStreamer(api_hook, _resolve_endpoint('api_endpoint_entrepot'))

    def _entrepot_fetcher(api_hook):
        from amue.domain.entrepot_structure_fetcher import EntrepotStructureFetcher
        return EntrepotStructureFetcher(api_hook, _resolve_endpoint('api_endpoint_entrepot'))

    return {
        'cdv': {
            'status':   _cdv_status,
            'streamer': _cdv_streamer,
            'fetcher':  _cdv_fetcher,
        },
        'entrepot': {
            'status':   _entrepot_status,
            'streamer': _entrepot_streamer,
            'fetcher':  _entrepot_fetcher,
        },
    }


_SOURCE_REGISTRY: dict = {}


def _dispatch(component: str, api_hook):
    global _SOURCE_REGISTRY
    if not _SOURCE_REGISTRY:
        _SOURCE_REGISTRY = _build_registry()

    source = _get_source()
    entry = _SOURCE_REGISTRY.get(source)
    if entry is None:
        raise AirflowException(
            f"amue_api_source='{source}' non reconnue. "
            f"Sources disponibles : {sorted(_SOURCE_REGISTRY)}"
        )
    return entry[component](api_hook)


def get_status_checker(api_hook):
    """Retourne le StatusChecker de la source active."""
    return _dispatch('status', api_hook)


def get_data_streamer(api_hook):
    """Retourne le DataStreamer de la source active, endpoint résolu en interne."""
    return _dispatch('streamer', api_hook)


def get_structure_fetcher(api_hook):
    """Retourne le StructureFetcher de la source active, endpoint résolu en interne."""
    return _dispatch('fetcher', api_hook)
