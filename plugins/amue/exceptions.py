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
│   └── AMUEDataError      # Erreur de données (format, validation)
├── AMUESchemaError        # Erreurs de structure/schéma
│   ├── AMUETableNotFoundError    # Table non trouvée
│   └── AMUEStructureChangedError # Changement de structure détecté
└── AMUEBlueGreenError     # Erreurs Blue/Green
    ├── ConcurrentImportError     # Import concurrent détecté
    └── ViewSwitchError           # Erreur lors du switch des vues

================================================================================
USAGE
================================================================================

    from amue.exceptions import AMUEImportError, AMUEBatchError

    try:
        import_data(table_name)
    except AMUEBatchError as e:
        logger.error(f"Erreur batch {e.batch_num} sur {e.table_name}: {e}")
    except AMUEImportError as e:
        logger.error(f"Erreur import {e.table_name}: {e}")

================================================================================
"""
from typing import Optional, Any, Dict
from datetime import datetime


class AMUEError(Exception):
    """
    Exception de base pour toutes les erreurs AMUE.

    Attributes:
        message: Message d'erreur
        timestamp: Date/heure de l'erreur
        correlation_id: ID de corrélation pour le tracing
        context: Contexte additionnel (dict)
    """

    def __init__(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.timestamp = datetime.now()
        self.correlation_id = correlation_id
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        base = self.message
        if self.correlation_id:
            base = f"[{self.correlation_id}] {base}"
        return base

    def to_dict(self) -> Dict[str, Any]:
        """Convertit l'exception en dictionnaire pour le logging structuré"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id,
            'context': self.context
        }


# =============================================================================
# ERREURS API
# =============================================================================

class AMUEAPIError(AMUEError):
    """
    Erreur liée à l'API AMUE (réseau, auth, réponse).

    Attributes:
        status_code: Code HTTP de la réponse (si applicable)
        endpoint: Endpoint API concerné
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        endpoint: Optional[str] = None,
        **kwargs
    ):
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message, **kwargs)
        self.context.update({
            'status_code': status_code,
            'endpoint': endpoint
        })


class AMUEAuthError(AMUEAPIError):
    """Erreur d'authentification OAuth (token invalide, expiré, etc.)"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, status_code=401, **kwargs)


class AMUENetworkError(AMUEAPIError):
    """Erreur réseau (timeout, connexion refusée, DNS, etc.)"""

    def __init__(
        self,
        message: str,
        original_error: Optional[Exception] = None,
        **kwargs
    ):
        self.original_error = original_error
        super().__init__(message, **kwargs)
        self.context['original_error'] = str(original_error) if original_error else None


# =============================================================================
# ERREURS IMPORT
# =============================================================================

class AMUEImportError(AMUEError):
    """
    Erreur pendant l'import de données.

    Attributes:
        table_name: Nom de la table concernée
        rows_imported: Nombre de lignes importées avant l'erreur
    """

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        rows_imported: int = 0,
        **kwargs
    ):
        self.table_name = table_name
        self.rows_imported = rows_imported
        super().__init__(message, **kwargs)
        self.context.update({
            'table_name': table_name,
            'rows_imported': rows_imported
        })


class AMUEBatchError(AMUEImportError):
    """
    Erreur sur un batch spécifique pendant l'import.

    Attributes:
        batch_num: Numéro du batch en erreur
        batch_size: Taille du batch
    """

    def __init__(
        self,
        message: str,
        batch_num: Optional[int] = None,
        batch_size: Optional[int] = None,
        **kwargs
    ):
        self.batch_num = batch_num
        self.batch_size = batch_size
        super().__init__(message, **kwargs)
        self.context.update({
            'batch_num': batch_num,
            'batch_size': batch_size
        })


class AMUEDataError(AMUEImportError):
    """Erreur de données (format invalide, validation échouée, etc.)"""

    def __init__(
        self,
        message: str,
        column_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        **kwargs
    ):
        self.column_name = column_name
        self.invalid_value = invalid_value
        super().__init__(message, **kwargs)
        self.context.update({
            'column_name': column_name,
            'invalid_value': str(invalid_value) if invalid_value else None
        })


class AMUEDatabaseError(AMUEImportError):
    """
    Erreur de base de données pendant l'import.

    Attributes:
        sql_state: Code d'état SQL (si disponible)
        is_connection_error: True si c'est une erreur de connexion
    """

    def __init__(
        self,
        message: str,
        sql_state: Optional[str] = None,
        is_connection_error: bool = False,
        **kwargs
    ):
        self.sql_state = sql_state
        self.is_connection_error = is_connection_error
        super().__init__(message, **kwargs)
        self.context.update({
            'sql_state': sql_state,
            'is_connection_error': is_connection_error
        })


# =============================================================================
# ERREURS SCHÉMA
# =============================================================================

class AMUESchemaError(AMUEError):
    """Erreur liée au schéma ou à la structure des tables"""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs
    ):
        self.table_name = table_name
        self.schema_name = schema_name
        super().__init__(message, **kwargs)
        self.context.update({
            'table_name': table_name,
            'schema_name': schema_name
        })


class AMUETableNotFoundError(AMUESchemaError):
    """Table non trouvée dans la base de données"""
    pass


class AMUEStructureChangedError(AMUESchemaError):
    """
    Changement de structure détecté (fingerprint différent).

    Attributes:
        old_fingerprint: Ancienne empreinte
        new_fingerprint: Nouvelle empreinte
        changes: Description des changements détectés
    """

    def __init__(
        self,
        message: str,
        old_fingerprint: Optional[str] = None,
        new_fingerprint: Optional[str] = None,
        changes: Optional[str] = None,
        **kwargs
    ):
        self.old_fingerprint = old_fingerprint
        self.new_fingerprint = new_fingerprint
        self.changes = changes
        super().__init__(message, **kwargs)
        self.context.update({
            'old_fingerprint': old_fingerprint,
            'new_fingerprint': new_fingerprint,
            'changes': changes
        })


# =============================================================================
# ERREURS BLUE/GREEN
# =============================================================================

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


# =============================================================================
# UTILITAIRES
# =============================================================================

def is_retryable_error(error: Exception) -> bool:
    """
    Détermine si une erreur peut être retentée.

    Args:
        error: L'exception à analyser

    Returns:
        True si l'erreur est potentiellement transitoire et peut être retentée
    """
    # Erreurs réseau : toujours retryable
    if isinstance(error, AMUENetworkError):
        return True

    # Erreurs API avec certains codes
    if isinstance(error, AMUEAPIError):
        if error.status_code in (429, 500, 502, 503, 504):
            return True
        return False

    # Erreurs de connexion DB : retryable
    if isinstance(error, AMUEDatabaseError) and error.is_connection_error:
        return True

    # Erreurs d'import concurrent : retryable après délai
    if isinstance(error, ConcurrentImportError):
        return True

    # Autres erreurs AMUE : non retryable par défaut
    if isinstance(error, AMUEError):
        return False

    # Exceptions Python standard potentiellement retryables
    retryable_types = (TimeoutError, ConnectionError, ConnectionResetError)
    return isinstance(error, retryable_types)


def get_error_category(error: Exception) -> str:
    """
    Retourne la catégorie d'erreur pour les métriques.

    Args:
        error: L'exception à catégoriser

    Returns:
        Catégorie sous forme de chaîne
    """
    if isinstance(error, AMUEAuthError):
        return 'auth'
    elif isinstance(error, AMUENetworkError):
        return 'network'
    elif isinstance(error, AMUEAPIError):
        return 'api'
    elif isinstance(error, AMUEDatabaseError):
        return 'database'
    elif isinstance(error, AMUEBatchError):
        return 'batch'
    elif isinstance(error, AMUEImportError):
        return 'import'
    elif isinstance(error, AMUESchemaError):
        return 'schema'
    elif isinstance(error, AMUEBlueGreenError):
        return 'bluegreen'
    elif isinstance(error, AMUEError):
        return 'amue'
    else:
        return 'unknown'
