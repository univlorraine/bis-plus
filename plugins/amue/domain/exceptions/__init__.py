"""
Hiérarchie d'exceptions pour le module AMUE.

================================================================================
STRUCTURE DES EXCEPTIONS
================================================================================

AMUEError (base)
├── AMUEAPIError           # Erreurs liées à l'API (réseau, auth, etc.)
│   ├── AMUEAuthError      # Erreurs d'authentification OAuth
│   └── AMUENetworkError   # Erreurs réseau (timeout, connexion)
├── AMUEImportError        # Erreurs pendant l'import de données
│   ├── AMUEBatchError     # Erreur sur un batch spécifique
│   ├── AMUEDataError      # Erreur de données (format, validation)
│   └── AMUEDatabaseError  # Erreur de base de données
├── AMUESchemaError        # Erreurs de structure/schéma
│   ├── AMUETableNotFoundError    # Table non trouvée (AMUEError)
│   └── AMUEStructureChangedError # Changement de structure détecté
└── AMUEBlueGreenError     # Erreurs Blue/Green
    ├── ConcurrentImportError     # Import concurrent détecté
    └── ViewSwitchError           # Erreur lors du switch des vues

TableNotFoundError(AirflowException)  # Table absente du statut API (Airflow-native)

================================================================================
ORGANISATION DU PACKAGE
================================================================================

    exceptions/
      base.py      ← AMUEError
      api.py       ← AMUEAPIError, AMUEAuthError, AMUENetworkError
      import_.py   ← AMUEImportError, AMUEBatchError, AMUEDataError, AMUEDatabaseError
      schema.py    ← AMUESchemaError, AMUETableNotFoundError, AMUEStructureChangedError,
                      TableNotFoundError
      bluegreen.py ← AMUEBlueGreenError, ConcurrentImportError, ViewSwitchError
      utils.py     ← is_retryable_error(), get_error_category()

================================================================================
USAGE
================================================================================

    from amue.domain.exceptions import AMUEImportError, AMUEBatchError

    try:
        import_data(table_name)
    except AMUEBatchError as e:
        logger.error(f"Erreur batch {e.batch_num} sur {e.table_name}: {e}")
    except AMUEImportError as e:
        logger.error(f"Erreur import {e.table_name}: {e}")

================================================================================
"""
from amue.domain.exceptions.base import AMUEError
from amue.domain.exceptions.api import AMUEAPIError, AMUEAuthError, AMUENetworkError
from amue.domain.exceptions.import_ import (
    AMUEImportError,
    AMUEBatchError,
    AMUEDataError,
    AMUEDatabaseError,
)
from amue.domain.exceptions.schema import (
    AMUESchemaError,
    AMUETableNotFoundError,
    AMUEStructureChangedError,
    TableNotFoundError,
)
from amue.domain.exceptions.bluegreen import (
    AMUEBlueGreenError,
    ConcurrentImportError,
    ViewSwitchError,
)
from amue.domain.exceptions.error_classification import is_retryable_error, get_error_category

__all__ = [
    # Base
    "AMUEError",
    # API
    "AMUEAPIError",
    "AMUEAuthError",
    "AMUENetworkError",
    # Import
    "AMUEImportError",
    "AMUEBatchError",
    "AMUEDataError",
    "AMUEDatabaseError",
    # Schéma
    "AMUESchemaError",
    "AMUETableNotFoundError",
    "AMUEStructureChangedError",
    "TableNotFoundError",
    # Blue/Green
    "AMUEBlueGreenError",
    "ConcurrentImportError",
    "ViewSwitchError",
    # Utilitaires
    "is_retryable_error",
    "get_error_category",
]
