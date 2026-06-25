"""
Surface publique du plugin AMUE.

Ce module n'expose que les types, exceptions et la configuration. Tout le
reste (opérateurs, services, hooks, utilitaires) doit être importé depuis
son chemin canonique :

    from amue.application.pipeline.data_importer import AMUEDataImporter
    from amue.application.polling_service import AMUEPollingService
    from amue.infrastructure.notifications import send_failure_notification

Cela évite le couplage circulaire amue↔common qui rendait nécessaire un
mécanisme de lazy loading dans les versions précédentes.
"""

# Types
from amue.domain.types_amue import (
    TableInfo,
    TableInfoPartial,
    ColumnInfo,
    ImportResult,
    ImportResultPartial,
    BlueGreenStateDict,
    TableManagementResultDict,
    StructureInfo,
    ImportConfig,
    BatchResult,
    LockInfo,
    TableName,
    SchemaName,
    ColumnName,
    PrimaryKeyList,
    CorrelationId,
)

# Exceptions
from amue.domain.exceptions import (
    AMUEError,
    AMUEAPIError,
    AMUEAuthError,
    AMUENetworkError,
    AMUEImportError,
    AMUEBatchError,
    AMUEDataError,
    AMUEDatabaseError,
    AMUESchemaError,
    AMUETableNotFoundError,
    AMUEStructureChangedError,
    AMUEBlueGreenError,
    ConcurrentImportError,
    ViewSwitchError,
    is_retryable_error,
    get_error_category,
)

# Configuration
from amue.infrastructure.config.settings import AMUEConfig, AMUEDefaults, get_config, reload_config


__all__ = [
    # Types
    "TableInfo",
    "TableInfoPartial",
    "ColumnInfo",
    "ImportResult",
    "ImportResultPartial",
    "BlueGreenStateDict",
    "TableManagementResultDict",
    "StructureInfo",
    "ImportConfig",
    "BatchResult",
    "LockInfo",
    "TableName",
    "SchemaName",
    "ColumnName",
    "PrimaryKeyList",
    "CorrelationId",
    # Exceptions
    "AMUEError",
    "AMUEAPIError",
    "AMUEAuthError",
    "AMUENetworkError",
    "AMUEImportError",
    "AMUEBatchError",
    "AMUEDataError",
    "AMUEDatabaseError",
    "AMUESchemaError",
    "AMUETableNotFoundError",
    "AMUEStructureChangedError",
    "AMUEBlueGreenError",
    "ConcurrentImportError",
    "ViewSwitchError",
    "is_retryable_error",
    "get_error_category",
    # Configuration
    "AMUEConfig",
    "AMUEDefaults",
    "get_config",
    "reload_config",
]
