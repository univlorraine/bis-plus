from amue.hooks.amue_api_hook import AMUEAPIHook
# Types
from amue.types_amue import (
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
from amue.exceptions import (
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
# Systeme de notifications unifie
from amue.notifications import (
    EmailService,
    EmailConfig,
    Email,
    NotificationTemplates,
    NotificationService,
    send_failure_notification,
    send_success_notification,
    dag_failure_rollback,
)
from amue.notifications.report_generator import AMUEReportGenerator
# Operators - importés en lazy pour éviter les imports circulaires avec common.*
# (accessible via `from amue import AMUEDataImporter` ou via le chemin complet)
_LAZY_OPERATORS = {
    'AMUEDataImporter': ('amue.operators.pipeline.data_importer', 'AMUEDataImporter'),
    'AMUEDataStreamer': ('amue.operators.pipeline.data_streamer', 'AMUEDataStreamer'),
    'AMUEBatchInserter': ('common.operators.batch_inserter', 'AMUEBatchInserter'),
    'DuplicateDetector': ('amue.operators.pipeline.duplicate_detector', 'DuplicateDetector'),
    'AMUETableFilter': ('amue.operators.table_management.table_filter', 'AMUETableFilter'),
    'AMUETableManager': ('amue.operators.table_management.table_manager', 'AMUETableManager'),
    'AMUETableVerifier': ('amue.operators.table_management.table_verifier', 'AMUETableVerifier'),
}
# Services
from common.services.admin_state_manager import AdminStateManager
from amue.services.metadata_manager import AMUEMetadataManager
from amue.services.table_config_manager import TableConfigManager
from amue.services.api.polling_service import AMUEPollingService
from common.services.retry_service import (
    RetryService,
    RetryConfig,
    RetryStrategy,
    RetryResult,
    ErrorCategory,
    get_retry_service,
)
from amue.services.api.status_checker import AMUEStatusChecker
# Services Blue/Green
from common.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
from common.services.bluegreen.view_switcher import ViewSwitcher
from common.services.bluegreen.schema_synchronizer import SchemaSynchronizer
# Utils
from common.utils.config.airflow_helpers import AirflowVariableManager
from common.utils.database.hooks import HookManager, create_postgres_hook, create_api_hook, create_bluegreen_hook
from amue.utils.config.settings import AMUEConfig, get_config, reload_config, Defaults
from common.utils.database.schema_utils import SchemaQualifier
from common.utils.database.connection_manager import PostgresConnectionManager
from amue.utils.tracing import (
    generate_correlation_id,
    generate_run_id,
    MemoryTracker,
    OperationTimer,
    TracingContext,
    trace_operation,
)
from amue.utils.transformers import (
    parse_column_definition,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints,
    validate_table_name,
    validate_column_name,
    validate_identifier,
)


def __getattr__(name):
    """Lazy loading des opérateurs pour éviter les imports circulaires avec common.*"""
    import importlib
    if name in _LAZY_OPERATORS:
        module_path, attr = _LAZY_OPERATORS[name]
        mod = importlib.import_module(module_path)
        obj = getattr(mod, attr)
        globals()[name] = obj  # cache pour les accès suivants
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
    # Hooks
    "AMUEAPIHook",
    # Services
    "AdminStateManager",
    "AMUEStatusChecker",
    "AMUEPollingService",
    "AMUEMetadataManager",
    "TableConfigManager",
    # Services Blue/Green
    "BlueGreenManager",
    "BlueGreenState",
    "ViewSwitcher",
    "SchemaSynchronizer",
    # Retry
    "RetryService",
    "RetryConfig",
    "RetryStrategy",
    "RetryResult",
    "ErrorCategory",
    "get_retry_service",
    # Operators - Import
    "AMUEDataImporter",
    "AMUEDataStreamer",
    "AMUEBatchInserter",
    "DuplicateDetector",
    # Operators - Other
    "AMUETableFilter",
    "AMUETableVerifier",
    "AMUETableManager",
    # Notifications
    "EmailService",
    "EmailConfig",
    "Email",
    "NotificationTemplates",
    "NotificationService",
    "send_failure_notification",
    "send_success_notification",
    "dag_failure_rollback",
    "AMUEReportGenerator",
    # Utils - Transformers
    "parse_column_definition",
    "compute_structure_hash_with_pk",
    "format_primary_keys",
    "compare_fingerprints",
    # Utils - Validation
    "validate_table_name",
    "validate_column_name",
    "validate_identifier",
    # Utils - Helpers
    "AirflowVariableManager",
    "HookManager",
    "create_postgres_hook",
    "create_api_hook",
    "create_bluegreen_hook",
    # Schema Utils
    "SchemaQualifier",
    "PostgresConnectionManager",
    # Config
    "AMUEConfig",
    "get_config",
    "reload_config",
    "Defaults",
    # Tracing
    "generate_correlation_id",
    "generate_run_id",
    "MemoryTracker",
    "OperationTimer",
    "TracingContext",
    "trace_operation",
]
