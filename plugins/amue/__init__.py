from amue.hooks.amue_api_hook import AMUEAPIHook
# Nouveau système de notifications
from amue.notifications import (
    EmailService,
    ErrorNotifier,
    SuccessNotifier,
    send_failure_notification,
)
# Rétro-compatibilité avec l'ancien système
from amue.notifications.notification_service import NotificationService, ErrorContext
from amue.notifications.report_generator import AMUEReportGenerator
from amue.operators.data_importer import AMUEDataImporter
from amue.operators.table_filter import AMUETableFilter
from amue.operators.table_manager import AMUETableManager
from amue.operators.table_verifier import AMUETableVerifier
from amue.services.metadata_manager import AMUEMetadataManager
from amue.services.polling_service import AMUEPollingService
from amue.services.status_checker import AMUEStatusChecker
from amue.utils.airflow_helpers import AirflowVariableManager
from amue.utils.hooks import HookManager, create_postgres_hook, create_api_hook
from amue.utils.settings import AMUEConfig, get_config, reload_config
from amue.utils.transformers import (
    parse_column_definition,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints,
    validate_table_name,
    validate_column_name,
    validate_identifier,
)

__all__ = [
    # Hooks
    "AMUEAPIHook",
    # Services
    "AMUEStatusChecker",
    "AMUEPollingService",
    "AMUEMetadataManager",
    # Operators
    "AMUETableFilter",
    "AMUETableVerifier",
    "AMUETableManager",
    "AMUEDataImporter",
    # Notifications (nouveau)
    "EmailService",
    "ErrorNotifier",
    "SuccessNotifier",
    "send_failure_notification",
    # Notifications (rétro-compatibilité)
    "NotificationService",
    "ErrorContext",
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
    # Config
    "AMUEConfig",
    "get_config",
    "reload_config",
]
