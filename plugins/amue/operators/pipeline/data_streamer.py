# amue/operators/pipeline/data_streamer.py
"""
Streaming de donnees depuis l'API AMUE avec pagination.

Ce module gere la recuperation des donnees depuis l'API AMUE en mode streaming
(generateur Python) pour optimiser l'utilisation memoire lors du traitement
de gros volumes.
"""
import logging
from datetime import datetime
from typing import Any, Dict, Generator

from airflow.exceptions import AirflowException
from common.services.retry_service import get_retry_service, ErrorCategory

logger = logging.getLogger(__name__)


class AMUEDataStreamer:
    """
    Gere le streaming des donnees depuis l'API AMUE.

    Recupere les donnees page par page et les yield une ligne a la fois,
    permettant un traitement en flux sans charger toutes les donnees en memoire.

    Attributes:
        api_hook: Hook de connexion a l'API AMUE (OAuth)
        endpoint: URL de l'endpoint API (apres substitution)

    Example:
        >>> streamer = AMUEDataStreamer(api_hook, endpoint)
        >>> for row in streamer.stream_data('CSKS', import_config):
        ...     process(row)
    """

    def __init__(self, api_hook, endpoint: str):
        """
        Initialise le streamer de donnees.

        Args:
            api_hook: Instance de AMUEAPIHook pour les appels API
            endpoint: URL de l'endpoint API
        """
        self.api_hook = api_hook
        self.endpoint = endpoint

    def stream_data(
        self,
        table_name: str,
        import_config: Dict[str, Any]
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Recupere les donnees en streaming (generateur).

        Args:
            table_name: Nom de la table a recuperer
            import_config: Configuration d'import contenant:
                - import_type: "full" ou "delta"
                - delta: Nom de la colonne de date pour import differentiel
                - last_import: Date ISO du dernier import

        Yields:
            Dictionnaire representant une ligne de donnees

        Raises:
            AirflowException: Si la recuperation echoue apres les retries
        """
        base_params = self._build_query_params(table_name, import_config)
        skip = 0
        page = 1

        while True:
            params = base_params.copy()
            params['skip'] = skip

            rows, has_more = self._fetch_page(params, page)

            if not rows:
                break

            for row in rows:
                yield row

            if not has_more:
                break

            skip += len(rows)
            page += 1

    def _build_query_params(
        self,
        table_name: str,
        import_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Construit les parametres de requete"""
        params = {
            'nom': table_name.upper(),
            'f': 'json'
        }

        # Import differentiel
        import_type = import_config.get('import_type', 'full')
        delta_column = import_config.get('delta', '')
        last_import = import_config.get('last_import', '')

        if import_type == 'delta' and delta_column and last_import:
            last_import_str = self._format_date_for_query(last_import)
            # Utilise >= pour récupérer toutes les modifications depuis le dernier import
            params['q'] = f"{delta_column}>='{last_import_str}'"
            logger.info(f"Delta (plage): {params['q']}")

        return params

    def _format_date_for_query(self, date_str: str) -> str:
        """Formate une date pour la requete API"""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y%m%d')
        except (ValueError, AttributeError) as e:
            logger.warning(f"Format de date invalide '{date_str}': {e}")
            return date_str.replace('-', '')[:8]

    def _fetch_page(
        self,
        params: Dict[str, Any],
        page: int
    ) -> tuple[list, bool]:
        """
        Recupere une page de donnees avec retry intelligent.

        Utilise le RetryService pour appliquer des strategies differenciees
        selon le type d'erreur (4xx, 429, 5xx, timeout, etc.)

        Args:
            params: Parametres de la requete API
            page: Numero de page (pour le logging)

        Returns:
            Tuple (rows, has_more):
                - rows: Liste des lignes de la page
                - has_more: True s'il reste des pages a recuperer
        """
        retry_service = get_retry_service()

        def fetch_operation():
            logger.info(f"Page {page} (skip={params['skip']})")

            response = self.api_hook.call_api(self.endpoint, params)

            if not isinstance(response, dict) or 'data' not in response:
                raise ValueError("Format reponse invalide")

            return response

        def on_retry(attempt: int, error: Exception, delay: float):
            category = retry_service.categorize_error(error)
            logger.warning(
                f"[RETRY] Page {page} - Tentative {attempt + 1} echouee - "
                f"Type: {category.value} - Retry dans {delay:.1f}s"
            )

        result = retry_service.execute_with_retry(fetch_operation, on_retry)

        if not result.success:
            error_msg = self._build_error_message(result, retry_service)
            logger.error(f"[FETCH] {error_msg}")
            retry_info = retry_service.get_retry_info(result.error)
            logger.info(f"[FETCH] Recommandation: {retry_info['recommendation']}")
            raise AirflowException(error_msg)

        # Succes - traite la reponse
        response = result.result
        data_obj = response['data']
        rows = data_obj.get('row', [])

        if not isinstance(rows, list):
            rows = [rows] if rows else []

        if rows:
            logger.info(f"{len(rows)} lignes recuperees")

        if result.attempts > 1:
            logger.info(
                f"[FETCH] Succes apres {result.attempts} tentatives "
                f"(delai total: {result.total_delay:.1f}s)"
            )

        # Verifie s'il y a plus de donnees
        count = data_obj.get('count', 0)
        top = data_obj.get('top', 99)
        has_more = len(rows) >= top and (params['skip'] + len(rows)) < count

        return rows, has_more

    def _build_error_message(self, result, retry_service) -> str:
        """Construit un message d'erreur detaille selon la categorie"""
        category = result.error_category

        if category == ErrorCategory.CLIENT_ERROR:
            return (
                f"Erreur client (4xx) - Pas de retry automatique. "
                f"Verifiez les parametres: {result.error}"
            )
        elif category == ErrorCategory.RATE_LIMITED:
            return (
                f"Rate limit (429) atteint apres {result.attempts} tentatives. "
                f"Temps total d'attente: {result.total_delay:.1f}s"
            )
        elif category == ErrorCategory.SERVER_ERROR:
            return (
                f"Erreur serveur (5xx) persistante apres {result.attempts} tentatives. "
                f"L'API AMUE est peut-etre indisponible."
            )
        elif category == ErrorCategory.TIMEOUT:
            return (
                f"Timeout reseau apres {result.attempts} tentatives. "
                f"Verifiez la connectivite."
            )
        else:
            return (
                f"Impossible de recuperer les donnees apres {result.attempts} tentatives: "
                f"{result.error}"
            )
