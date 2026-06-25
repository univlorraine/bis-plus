"""Layer: domain

Exception de base pour la famille d'erreurs AMUE."""
from common.domain.exceptions import BaseError


class AMUEError(BaseError):
    """
    Exception racine pour toutes les erreurs AMUE.

    Hérite de `common.domain.exceptions.BaseError` (qui porte les attributs
    `message`, `timestamp`, `correlation_id`, `context`). Sert de marker :
    les sous-classes spécifiques AMUE (AMUEBatchError, AMUESchemaError, ...)
    héritent à la fois de leur catégorie commune et de cette classe.
    """
    pass
