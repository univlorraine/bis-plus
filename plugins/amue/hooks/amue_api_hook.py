"""
Hook personnalisé pour interagir avec l'API AMUE
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
from airflow.sdk import Connection

logger = logging.getLogger(__name__)


class AMUEAPIHook:
    """
    Hook pour gérer l'authentification OAuth et les appels à l'API AMUE

    Args:
        conn_id: ID de la connexion Airflow contenant les credentials OAuth

    Example:
        >>> hook = AMUEAPIHook()
        >>> data = hook.call_api('finances/cdv/v1/preprod/{CODE_UNIV}/table', {'nom': 'CSKS'})
    """

    def __init__(self):
        """
        Initialise le hook avec la connexion Airflow

        Args:
            conn_id: ID de la connexion Airflow
        """
        self.connection = Connection.get('oauth_api')
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

    def _is_token_expired(self) -> bool:
        """
        Vérifie si le token est expiré ou proche de l'expiration

        Returns:
            True si le token est expiré ou absent
        """
        if not self.access_token or not self.token_expires_at:
            return True
        return datetime.now() >= self.token_expires_at

    def get_oauth_token(self) -> str:
        """
        Obtient un token OAuth2 via le flow client_credentials

        Returns:
            Le token d'accès OAuth2

        Raises:
            ValueError: Si token_url est manquant dans la configuration
            requests.exceptions.RequestException: En cas d'erreur réseau
        """
        client_id = self.connection.login
        client_secret = self.connection.password

        # Parse les paramètres extra
        extra = self._parse_connection_extra()
        token_url = extra.get('token_url')

        if not token_url:
            raise ValueError(
                f"token_url manquant dans la connexion 'oauth_api'! "
                "Ajoutez-le dans Admin > Connections > Extra: "
                '{"token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token"}'
            )

        logger.info(f"[AUTH] Authentification OAuth: {token_url}")
        logger.info(f"[AUTH] Client ID: {client_id[:10]}***")

        # Prépare la requête OAuth2
        auth = (client_id, client_secret)
        data = {'grant_type': 'client_credentials'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(
                token_url,
                auth=auth,
                data=data,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data['access_token']

            expires_in = token_data.get('expires_in', 3600)
            token_type = token_data.get('token_type', 'Bearer')

            # Enregistre l'expiration avec marge de sécurité (90% du temps)
            if isinstance(expires_in, int):
                self.token_expires_at = datetime.now() + timedelta(seconds=int(expires_in * 0.9))
            else:
                self.token_expires_at = datetime.now() + timedelta(seconds=3240)  # 90% de 3600

            logger.info(f"[AUTH] Token obtenu - Type: {token_type}, Expire: {expires_in}s")
            logger.debug(f"[AUTH] Token valide jusqu'à: {self.token_expires_at}")

            return self.access_token

        except requests.exceptions.HTTPError as e:
            error_detail = e.response.text if e.response else 'N/A'
            logger.error(f"[ERROR] Erreur HTTP {e.response.status_code if e.response else 'N/A'}")
            logger.error(f"[ERROR] Detail: {error_detail}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[ERROR] Erreur de connexion: {e}")
            raise
        except KeyError as e:
            logger.error(f"[ERROR] Format de reponse OAuth invalide: champ manquant {e}")
            raise ValueError(f"Reponse OAuth invalide: {e}")

    def call_api(
            self,
            endpoint: str,
            params: Optional[Dict] = None,
            check_status_only: bool = False,
            timeout: int = 60
    ) -> Any:
        """
        Effectue un appel GET à l'API AMUE avec authentification OAuth

        Args:
            endpoint: Chemin de l'endpoint (ex: 'finances/cdv/v1/preprod/ul/table')
            params: Paramètres query string optionnels
            check_status_only: Si True, retourne seulement le code HTTP
            timeout: Timeout en secondes pour l'appel API (défaut: 60)

        Returns:
            - Si check_status_only=True: code HTTP (int)
            - Si réponse JSON: dict ou list parsé
            - Sinon: texte brut (str)

        Raises:
            requests.exceptions.RequestException: En cas d'erreur réseau
        """
        # Obtient le token si nécessaire ou si expiré
        if self._is_token_expired():
            logger.debug("[API] Token absent ou expiré, renouvellement...")
            self.get_oauth_token()

        # Construit l'URL complète
        extra = self._parse_connection_extra()
        api_base_url = (
                extra.get('api_base_url') or
                self.connection.host or
                'https://sandbox.api.amue.fr'
        )

        endpoint = endpoint.lstrip('/')
        url = f"{api_base_url.rstrip('/')}/{endpoint}"

        # Prépare les headers
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
        }

        logger.info(f"[API] Appel: {url}")
        if params:
            logger.info(f"[API] Params: {params}")

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )

            # Mode vérification status uniquement
            if check_status_only:
                return response.status_code

            response.raise_for_status()

            # Tente de parser en JSON
            try:
                return response.json()
            except json.JSONDecodeError:
                logger.info("[API] Reponse en texte brut (non JSON)")
                return response.text

        except requests.exceptions.HTTPError as e:
            # Mode vérification status
            if check_status_only:
                return e.response.status_code if e.response else 500

            # Gestion du token expiré (401)
            if e.response and e.response.status_code == 401:
                logger.info("[API] Token expire, renouvellement...")
                self.access_token = None
                self.get_oauth_token()
                # Retry avec le nouveau token
                return self.call_api(endpoint, params, check_status_only)

            raise

    def _parse_connection_extra(self) -> Dict:
        """
        Parse le champ extra de la connexion Airflow

        Returns:
            Dictionnaire des paramètres extra ou dict vide
        """
        if not self.connection.extra:
            return {}

        try:
            return json.loads(self.connection.extra)
        except json.JSONDecodeError:
            logger.warning("[WARN] Impossible de parser connection.extra en JSON")
            return {}
