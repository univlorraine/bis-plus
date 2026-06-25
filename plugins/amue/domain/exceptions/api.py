"""Layer: domain

Exceptions liées à l'API AMUE : AMUEAPIError, AMUEAuthError, AMUENetworkError."""
from typing import Optional

from amue.domain.exceptions.base import AMUEError


class AMUEAPIError(AMUEError):
    """
    Erreur liée à l'API AMUE (réseau, auth, réponse).

    Attributes:
        status_code: Code HTTP de la réponse (si applicable)
        endpoint: Endpoint API concerné
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        endpoint: Optional[str] = None,
        **kwargs
    ):
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message, **kwargs)
        self.context.update({
            'status_code': status_code,
            'endpoint': endpoint
        })


class AMUEAuthError(AMUEAPIError):
    """Erreur d'authentification OAuth (token invalide, expiré, etc.)"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, status_code=401, **kwargs)


class AMUENetworkError(AMUEAPIError):
    """Erreur réseau (timeout, connexion refusée, DNS, etc.)"""

    def __init__(
        self,
        message: str,
        original_error: Optional[Exception] = None,
        **kwargs
    ):
        self.original_error = original_error
        super().__init__(message, **kwargs)
        self.context['original_error'] = str(original_error) if original_error else None
