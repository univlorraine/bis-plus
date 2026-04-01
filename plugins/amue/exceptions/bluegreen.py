"""Exceptions Blue/Green : AMUEBlueGreenError, ConcurrentImportError, ViewSwitchError."""
from typing import Optional

from amue.exceptions.base import AMUEError


class AMUEBlueGreenError(AMUEError):
    """Erreur liée à l'architecture Blue/Green"""

    def __init__(
        self,
        message: str,
        active_schema: Optional[str] = None,
        target_schema: Optional[str] = None,
        **kwargs
    ):
        self.active_schema = active_schema
        self.target_schema = target_schema
        super().__init__(message, **kwargs)
        self.context.update({
            'active_schema': active_schema,
            'target_schema': target_schema
        })


class ConcurrentImportError(AMUEBlueGreenError):
    """
    Import concurrent détecté.

    Levée quand un import tente de démarrer alors qu'un autre est en cours.
    """

    def __init__(
        self,
        message: str = "Un import est déjà en cours",
        import_started_at: Optional[str] = None,
        **kwargs
    ):
        self.import_started_at = import_started_at
        super().__init__(message, **kwargs)
        self.context['import_started_at'] = import_started_at


class ViewSwitchError(AMUEBlueGreenError):
    """Erreur lors du switch des vues"""

    def __init__(
        self,
        message: str,
        failed_view: Optional[str] = None,
        **kwargs
    ):
        self.failed_view = failed_view
        super().__init__(message, **kwargs)
        self.context['failed_view'] = failed_view
