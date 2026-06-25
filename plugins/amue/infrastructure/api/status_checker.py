"""
Layer: infrastructure

Gestionnaire de vérification des statuts AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module interroge l'API AMUE pour récupérer le statut des tables disponibles.
Il est utilisé à deux moments :
    1. POLLING : Vérifier si l'API est accessible et le traitement terminé
    2. FILTRAGE : Récupérer la liste des tables disponibles et leur statut

================================================================================
STRUCTURE DE LA RÉPONSE API
================================================================================

L'API AMUE renvoie une réponse JSON avec la structure suivante :

{
    "finish": "2024-01-15 03:45:00",  # Date/heure de fin du traitement AMUE
    "nbtables": 25,                    # Nombre total de tables
    "nbtables_ko": 0,                  # Nombre de tables en erreur
    "status": [
        {
            "name": "CSKS",            # Nom de la table
            "status": "OK",            # Statut ("OK" ou "KO")
            "mode": "FULL",            # Mode d'export AMUE
            "count": 15000,            # Nombre de lignes
            "row_size": 256            # Taille moyenne d'une ligne
        },
        ...
    ]
}

================================================================================
VARIABLE 'FINISH'
================================================================================

La variable 'finish' est CRITIQUE pour le fonctionnement du DAG :
    - Si VIDE ou ABSENTE : Le traitement AMUE est EN COURS
    - Si RENSEIGNÉE : Le traitement AMUE est TERMINÉ, on peut importer

Le polling attend que 'finish' soit renseigné avant de continuer.

================================================================================
CONFIGURATION
================================================================================

Variables Airflow :
    - universite : Code université pour l'endpoint
    - api_endpoint_admin : Template URL admin avec $univ

================================================================================
"""
import logging
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class AMUEStatusChecker:
    """
    Gère la vérification des statuts de l'API AMUE pour le polling et le filtrage.
    """

    def __init__(self, api_hook):
        """
        Initialise le vérificateur de statuts

        Args:
            api_hook: Hook API AMUE
        """
        self.api_hook = api_hook

        univ = VarMgr.get_required(
            'universite',
            "La variable 'univ' doit être définie pour initialiser AMUEStatusChecker",
        )
        endpointadm = VarMgr.get_required(
            'api_endpoint_admin',
            "La variable 'api_endpoint_admin' doit être définie pour initialiser AMUEStatusChecker",
        )
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

    def get_current_status(self) -> Dict:
        """
        Récupère le statut actuel de l'API

        Returns:
            Dictionnaire des statuts par table
        """
        logger.info("[STATUS] Récupération statut actuel (API)")

        params = {'status': ''}
        response = self.api_hook.call_api(self.endpoint, params)

        if not isinstance(response, dict) or 'status' not in response:
            raise ValueError("Format réponse invalide")

        tables_status = self._parse_tables_status(response.get('status', []))

        logger.info(f"[STATUS] {len(tables_status)} tables trouvées")

        return tables_status

    def fetch_full_status(self) -> Dict:
        """
        Récupère le statut complet en UN SEUL appel API.

        Cette méthode récupère le code HTTP, le timestamp finish et le statut
        de toutes les tables en un seul appel, optimisant ainsi les performances
        du polling.

        Returns:
            Dict avec:
            - 'http_status': int (200, 5xx, etc.)
            - 'finish': str ou None
            - 'tables_status': Dict[str, Dict] (parsed)
            - 'raw_response': Dict (réponse complète)

        Raises:
            AirflowException: En cas d'erreur critique (4xx sauf 429)
        """
        logger.info("[STATUS] Récupération statut complet (un seul appel)")
        params = {'status': ''}

        try:
            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict):
                logger.warning("[WARN] Réponse non-JSON lors de la récupération du statut")
                return {
                    'http_status': 200,
                    'finish': None,
                    'tables_status': {},
                    'raw_response': response
                }

            finish_value = response.get('finish')
            tables_status = self._parse_tables_status(response.get('status', []))

            logger.info(f"[STATUS] HTTP 200, finish={finish_value or 'non renseigné'}, "
                        f"{len(tables_status)} tables")

            return {
                'http_status': 200,
                'finish': finish_value,
                'start': response.get('start'),
                'tables_status': tables_status,
                'raw_response': response
            }

        except Exception as e:
            # Extrait le code HTTP de l'erreur si disponible
            http_status = getattr(getattr(e, 'response', None), 'status_code', 500)
            logger.warning(f"[STATUS] Erreur HTTP {http_status}: {str(e)}")

            return {
                'http_status': http_status,
                'finish': None,
                'tables_status': {},
                'raw_response': None,
                'error': str(e)
            }

    def _parse_tables_status(self, status_list: List) -> Dict:
        """Parse la liste des statuts de tables"""
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
                'finish': table_info.get('finish', None)
            }

        return tables_status
