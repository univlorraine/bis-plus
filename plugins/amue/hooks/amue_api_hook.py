"""
Hook personnalisé pour interagir avec l'API AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce hook encapsule toute la communication avec l'API AMUE :
    - Authentification OAuth2 (client_credentials flow)
    - Gestion automatique du token (renouvellement)
    - Appels API avec retry intelligent

================================================================================
AUTHENTIFICATION OAUTH2
================================================================================

L'API AMUE utilise le flow OAuth2 "client_credentials" :

    ┌─────────────┐                      ┌───────────────┐
    │   Airflow   │ ──── Credentials ───►│  Auth Server  │
    │   (client)  │                      │ (AMUE OAuth)  │
    │             │ ◄──── Token ─────────│               │
    └──────┬──────┘                      └───────────────┘
           │
           │ Token Bearer
           ▼
    ┌───────────────┐
    │   API AMUE    │
    │  (ressources) │
    └───────────────┘

Le token est mis en cache et renouvelé automatiquement :
    - Avant expiration (marge de 10%)
    - Après une erreur 401 (token invalide)

================================================================================
CONFIGURATION AIRFLOW
================================================================================

Connexion Airflow : 'oauth_api'

Champs requis :
    - login     : Client ID OAuth
    - password  : Client Secret OAuth
    - extra     : JSON avec configuration
        {
            "token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token",
            "api_base_url": "https://sandbox.api.amue.fr"
        }

================================================================================
RETRY INTELLIGENT
================================================================================

Les appels API utilisent le RetryService pour adapter le comportement
selon le type d'erreur (voir retry_service.py pour les détails).

Exception : check_status_only=True désactive le retry (utilisé par le polling
pour simplement vérifier si l'API répond).

================================================================================
USAGE
================================================================================

    >>> from amue.hooks.amue_api_hook import AMUEAPIHook
    >>>
    >>> hook = AMUEAPIHook()
    >>>
    >>> # Appel simple
    >>> data = hook.call_api('endpoint/path', {'param': 'value'})
    >>>
    >>> # Vérification de disponibilité (polling)
    >>> status_code = hook.call_api('endpoint/path', check_status_only=True)

================================================================================
"""
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
from airflow.sdk import Connection

from amue.services.retry_service import get_retry_service, ErrorCategory

logger = logging.getLogger(__name__)


# =============================================================================
# CACHE DE TOKEN GLOBAL (Thread-Safe)
# =============================================================================
# Le cache est partagé entre toutes les instances de AMUEAPIHook pour éviter
# de demander plusieurs tokens en parallèle.
# =============================================================================

class _TokenCache:
    """Cache thread-safe pour le token OAuth."""

    def __init__(self):
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    def get_token(self) -> tuple[Optional[str], bool]:
        """
        Récupère le token du cache.

        Returns:
            Tuple (token, is_valid): token et si il est encore valide
        """
        with self._lock:
            if not self._token or not self._expires_at:
                return None, False

            is_valid = datetime.now() < self._expires_at
            return self._token, is_valid

    def set_token(self, token: str, expires_in_seconds: int) -> None:
        """
        Stocke le token dans le cache.

        Args:
            token: Token d'accès
            expires_in_seconds: Durée de validité en secondes
        """
        with self._lock:
            self._token = token
            # Marge de sécurité de 10%
            safe_duration = int(expires_in_seconds * 0.9)
            self._expires_at = datetime.now() + timedelta(seconds=safe_duration)

    def invalidate(self) -> None:
        """Invalide le token en cache."""
        with self._lock:
            self._token = None
            self._expires_at = None


# Instance globale du cache
_token_cache = _TokenCache()


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
        Initialise le hook avec la connexion Airflow.

        Le token OAuth est géré via un cache global thread-safe
        pour éviter les requêtes de token multiples en parallèle.
        """
        self.connection = Connection.get('oauth_api')
        self._token_cache = _token_cache  # Utilise le cache global

    def _is_token_expired(self) -> bool:
        """
        Vérifie si le token est expiré ou proche de l'expiration.

        Returns:
            True si le token est expiré ou absent
        """
        _, is_valid = self._token_cache.get_token()
        return not is_valid

    @property
    def access_token(self) -> Optional[str]:
        """Récupère le token d'accès du cache."""
        token, _ = self._token_cache.get_token()
        return token

    @access_token.setter
    def access_token(self, value: Optional[str]) -> None:
        """Invalide le cache si on assigne None."""
        if value is None:
            self._token_cache.invalidate()

    def get_oauth_token(self) -> str:
        """
        Obtient un token OAuth2 via le flow client_credentials.

        Le token est stocké dans un cache thread-safe partagé entre
        toutes les instances de AMUEAPIHook.

        Returns:
            Le token d'accès OAuth2

        Raises:
            ValueError: Si token_url est manquant dans la configuration
            requests.exceptions.RequestException: En cas d'erreur réseau

        Security:
            - Les credentials ne sont jamais loggés
            - Le token n'est jamais loggé
            - Seul le type de token et la durée de validité sont loggés
        """
        # Vérifie d'abord le cache (évite les requêtes inutiles)
        cached_token, is_valid = self._token_cache.get_token()
        if cached_token and is_valid:
            logger.debug("[AUTH] Token récupéré du cache")
            return cached_token

        client_id = self.connection.login
        client_secret = self.connection.password

        # Parse les paramètres extra
        extra = self._parse_connection_extra()
        token_url = extra.get('token_url')

        if not token_url:
            raise ValueError(
                "token_url manquant dans la connexion 'oauth_api'! "
                "Ajoutez-le dans Admin > Connections > Extra: "
                '{"token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token"}'
            )

        # SECURITY: Ne jamais logger les credentials ou le token
        logger.info("[AUTH] Demande de token OAuth...")

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
            access_token = token_data['access_token']

            expires_in = token_data.get('expires_in', 3600)
            token_type = token_data.get('token_type', 'Bearer')

            # Stocke dans le cache (avec marge de sécurité)
            if isinstance(expires_in, int):
                self._token_cache.set_token(access_token, expires_in)
            else:
                self._token_cache.set_token(access_token, 3600)

            # SECURITY: Ne loggue que les métadonnées, jamais le token
            logger.info(f"[AUTH] Token obtenu - Type: {token_type}, Validité: {expires_in}s")

            return access_token

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 'N/A'
            # SECURITY: Ne pas logger le body de réponse qui pourrait contenir des infos sensibles
            logger.error(f"[AUTH] Erreur HTTP {status_code} lors de l'authentification")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[AUTH] Erreur réseau: {type(e).__name__}")
            raise
        except KeyError as e:
            logger.error(f"[AUTH] Réponse OAuth invalide: champ manquant {e}")
            raise ValueError(f"Reponse OAuth invalide: {e}")

    def call_api(
            self,
            endpoint: str,
            params: Optional[Dict] = None,
            check_status_only: bool = False,
            timeout: int = 60,
            use_retry: bool = True
    ) -> Any:
        """
        Effectue un appel GET à l'API AMUE avec authentification OAuth

        Utilise le RetryService pour appliquer des stratégies de retry
        intelligentes selon le type d'erreur:
        - 4xx (sauf 429): Pas de retry (erreur client)
        - 429: Retry agressif (rate limit)
        - 5xx: Backoff exponentiel (erreur serveur)
        - Timeout: Retry court

        Args:
            endpoint: Chemin de l'endpoint (ex: 'finances/cdv/v1/preprod/ul/table')
            params: Paramètres query string optionnels
            check_status_only: Si True, retourne seulement le code HTTP
            timeout: Timeout en secondes pour l'appel API (défaut: 60)
            use_retry: Si True, utilise le retry intelligent (défaut: True)

        Returns:
            - Si check_status_only=True: code HTTP (int)
            - Si réponse JSON: dict ou list parsé
            - Sinon: texte brut (str)

        Raises:
            requests.exceptions.RequestException: En cas d'erreur réseau persistante
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
        appel = f"[API] Appel: {url}"
        if params:
            appel += f" + Params: {params}"
        logger.info(appel)
        if use_retry and not check_status_only:
            return self._call_api_with_retry(url, headers, params, timeout)
        else:
            return self._call_api_simple(url, headers, params, timeout, check_status_only, endpoint)

    def _call_api_simple(
            self,
            url: str,
            headers: Dict,
            params: Optional[Dict],
            timeout: int,
            check_status_only: bool,
            endpoint: str
    ) -> Any:
        """Appel API simple sans retry intelligent (pour check_status_only)"""
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )

            if check_status_only:
                return response.status_code

            response.raise_for_status()

            try:
                return response.json()
            except json.JSONDecodeError:
                logger.info("[API] Réponse en texte brut (non JSON)")
                return response.text

        except requests.exceptions.HTTPError as e:
            if check_status_only:
                return e.response.status_code if e.response else 500

            # Gestion du token expiré (401)
            if e.response and e.response.status_code == 401:
                logger.info("[API] Token expiré, renouvellement...")
                self.access_token = None
                self.get_oauth_token()
                return self.call_api(endpoint, params, check_status_only)

            raise

    def _call_api_with_retry(
            self,
            url: str,
            headers: Dict,
            params: Optional[Dict],
            timeout: int
    ) -> Any:
        """
        Appel API avec retry intelligent selon le type d'erreur

        Returns:
            Réponse JSON ou texte brut
        """
        retry_service = get_retry_service()
        token_refreshed = False

        def api_operation():
            nonlocal token_refreshed

            # Renouvelle le token si nécessaire (après une erreur 401)
            if token_refreshed:
                headers['Authorization'] = f'Bearer {self.access_token}'

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )

            # Gestion spéciale du 401 (token expiré)
            if response.status_code == 401 and not token_refreshed:
                logger.info("[API] Token expiré (401), renouvellement...")
                self.access_token = None
                self.get_oauth_token()
                token_refreshed = True
                # Relance pour retry avec nouveau token
                raise requests.exceptions.HTTPError(response=response)

            response.raise_for_status()

            try:
                return response.json()
            except json.JSONDecodeError:
                logger.info("[API] Réponse en texte brut (non JSON)")
                return response.text

        def on_retry(attempt: int, error: Exception, delay: float):
            category = retry_service.categorize_error(error)
            logger.warning(
                f"[API RETRY] Tentative {attempt + 1} échouée - "
                f"Type: {category.value} - Délai: {delay:.1f}s"
            )

            # Log spécifique selon le type d'erreur
            if category == ErrorCategory.RATE_LIMITED:
                logger.warning("[API RETRY] Rate limit (429) - L'API limite les requêtes")
            elif category == ErrorCategory.SERVER_ERROR:
                logger.warning("[API RETRY] Erreur serveur (5xx) - L'API AMUE rencontre des problèmes")
            elif category == ErrorCategory.TIMEOUT:
                logger.warning(f"[API RETRY] Timeout après {timeout}s")

        result = retry_service.execute_with_retry(api_operation, on_retry)

        if result.success:
            if result.attempts > 1:
                logger.info(
                    f"[API] Succès après {result.attempts} tentatives "
                    f"(délai total: {result.total_delay:.1f}s)"
                )
            return result.result

        # Échec - log détaillé
        category = result.error_category
        retry_info = retry_service.get_retry_info(result.error)

        logger.error(
            f"[API] Échec après {result.attempts} tentative(s) - "
            f"Type: {category.value if category else 'unknown'}"
        )
        logger.info(f"[API] Recommandation: {retry_info['recommendation']}")

        raise result.error

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
