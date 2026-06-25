"""Layer: domain

Exceptions d'import : AMUEImportError, AMUEBatchError, AMUEDataError, AMUEDatabaseError."""
from common.domain.exceptions import BatchError, DatabaseError, DataError, ImportError_

from amue.domain.exceptions.base import AMUEError


class AMUEImportError(ImportError_, AMUEError):
    """Erreur pendant l'import de données AMUE."""
    pass


class AMUEBatchError(BatchError, AMUEError):
    """Erreur sur un batch spécifique pendant l'import AMUE."""
    pass


class AMUEDataError(DataError, AMUEError):
    """Erreur de données AMUE (format invalide, validation échouée)."""
    pass


class AMUEDatabaseError(DatabaseError, AMUEError):
    """Erreur de base de données pendant l'import AMUE."""
    pass
