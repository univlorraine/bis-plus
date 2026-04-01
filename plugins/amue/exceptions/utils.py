"""Fonctions utilitaires pour la gestion des exceptions AMUE."""
from amue.exceptions.base import AMUEError
from amue.exceptions.api import AMUEAPIError, AMUENetworkError
from amue.exceptions.import_ import AMUEDatabaseError
from amue.exceptions.bluegreen import ConcurrentImportError


def is_retryable_error(error: Exception) -> bool:
    """
    Détermine si une erreur peut être retentée.

    Args:
        error: L'exception à analyser

    Returns:
        True si l'erreur est potentiellement transitoire et peut être retentée
    """
    # Erreurs réseau : toujours retryable
    if isinstance(error, AMUENetworkError):
        return True

    # Erreurs API avec certains codes
    if isinstance(error, AMUEAPIError):
        if error.status_code in (429, 500, 502, 503, 504):
            return True
        return False

    # Erreurs de connexion DB : retryable
    if isinstance(error, AMUEDatabaseError) and error.is_connection_error:
        return True

    # Erreurs d'import concurrent : retryable après délai
    if isinstance(error, ConcurrentImportError):
        return True

    # Autres erreurs AMUE : non retryable par défaut
    if isinstance(error, AMUEError):
        return False

    # Exceptions Python standard potentiellement retryables
    retryable_types = (TimeoutError, ConnectionError, ConnectionResetError)
    return isinstance(error, retryable_types)


def get_error_category(error: Exception) -> str:
    """
    Retourne la catégorie d'erreur pour les métriques.

    Args:
        error: L'exception à catégoriser

    Returns:
        Catégorie sous forme de chaîne
    """
    from amue.exceptions.api import AMUEAuthError, AMUENetworkError, AMUEAPIError
    from amue.exceptions.import_ import AMUEDatabaseError, AMUEBatchError, AMUEImportError
    from amue.exceptions.schema import AMUESchemaError
    from amue.exceptions.bluegreen import AMUEBlueGreenError

    if isinstance(error, AMUEAuthError):
        return 'auth'
    elif isinstance(error, AMUENetworkError):
        return 'network'
    elif isinstance(error, AMUEAPIError):
        return 'api'
    elif isinstance(error, AMUEDatabaseError):
        return 'database'
    elif isinstance(error, AMUEBatchError):
        return 'batch'
    elif isinstance(error, AMUEImportError):
        return 'import'
    elif isinstance(error, AMUESchemaError):
        return 'schema'
    elif isinstance(error, AMUEBlueGreenError):
        return 'bluegreen'
    elif isinstance(error, AMUEError):
        return 'amue'
    else:
        return 'unknown'
