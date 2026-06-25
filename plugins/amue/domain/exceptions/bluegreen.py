"""Layer: domain

Exceptions Blue/Green : AMUEBlueGreenError + re-exports communs."""
from common.domain.exceptions import (
    BlueGreenError,
    ConcurrentImportError,
    ViewSwitchError,
)

from amue.domain.exceptions.base import AMUEError


class AMUEBlueGreenError(BlueGreenError, AMUEError):
    """Erreur Blue/Green côté AMUE."""
    pass


# Ces deux exceptions ne sont plus AMUE-spécifiques (vivent dans common.domain.exceptions
# pour qu'ECC et common/services/bluegreen puissent les lever sans dépendre d'amue),
# mais sont re-exportées pour préserver les imports `from amue.domain.exceptions import …`.
__all__ = [
    'AMUEBlueGreenError',
    'ConcurrentImportError',
    'ViewSwitchError',
]
