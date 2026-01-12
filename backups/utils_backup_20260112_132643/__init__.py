"""
Package utils pour les DAGs AMUE
Contient toutes les classes et fonctions réutilisables
"""

# Import des hooks et utilitaires de base
from .amue_api_hook import AMUEAPIHook

# Import des transformateurs et utilitaires
from .amue_utils import (
    parse_column_definition,
    compute_structure_hash,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints
)

# Import des notifications
from .amue_notification_utils import (
    NotificationService,
    ErrorContext,
    send_failure_notification
)

# Import des services métier
from .amue_status_checker import AMUEStatusChecker
from .amue_table_filter import AMUETableFilter
from .amue_table_verifier import AMUETableVerifier
from .amue_table_manager import AMUETableManager
from .amue_data_importer import AMUEDataImporter
from .amue_polling_service import AMUEPollingService
from .amue_metadata_manager import AMUEMetadataManager
from .amue_report_generator import AMUEReportGenerator

__all__ = [
    # Hooks et utilitaires
    'AMUEAPIHook',
    'parse_column_definition',
    'compute_structure_hash',
    'compute_structure_hash_with_pk',
    'format_primary_keys',
    'compare_fingerprints',
    'send_failure_notification',
    'NotificationService',
    'ErrorContext',

    # Services métier
    'AMUEStatusChecker',
    'AMUETableFilter',
    'AMUETableVerifier',
    'AMUETableManager',
    'AMUEDataImporter',
    'AMUEPollingService',
    'AMUEMetadataManager',
    'AMUEReportGenerator',
]