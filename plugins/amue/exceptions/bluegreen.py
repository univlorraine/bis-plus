"""Exceptions Blue/Green : AMUEBlueGreenError + re-exports communs."""
from common.exceptions import (
    BlueGreenError,
    ConcurrentImportError,
    ViewSwitchError,
)

from amue.exceptions.base import AMUEError


class AMUEBlueGreenError(BlueGreenError, AMUEError):
    """Erreur Blue/Green côté AMUE."""
    pass


# Ces deux exceptions ne sont plus AMUE-spécifiques (vivent dans common.exceptions
# pour qu'ECC et common/services/bluegreen puissent les lever sans dépendre d'amue),
# mais sont re-exportées pour préserver les imports `from amue.exceptions import …`.
__all__ = [
    'AMUEBlueGreenError',
    'ConcurrentImportError',
    'ViewSwitchError',
]
