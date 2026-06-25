"""
Layer: domain

Hiérarchie d'exceptions partagée AMUE / ECC.

Les classes ici ne portent ni préfixe AMUE ni ECC : elles décrivent des
catégories d'erreurs neutres (`BatchError`, `DatabaseError`, ...) que les
plugins concrets peuvent spécialiser. Le but est que `plugins/common/`
n'ait jamais besoin d'importer depuis `plugins/amue/` ou `plugins/ecc/`.

AMUE conserve sa propre hiérarchie (`AMUEError`, `AMUEBatchError`, ...)
qui hérite de ces classes communes pour la rétro-compatibilité.
"""
from datetime import datetime
from typing import Any, Dict, Optional


class BaseError(Exception):
    """Exception racine pour AMUE et ECC."""

    def __init__(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.timestamp = datetime.now()
        self.correlation_id = correlation_id
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.correlation_id:
            return f"[{self.correlation_id}] {self.message}"
        return self.message

    def to_dict(self) -> Dict[str, Any]:
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id,
            'context': self.context,
        }


class ImportError_(BaseError):
    """Erreur pendant l'import de données."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        rows_imported: int = 0,
        **kwargs,
    ):
        self.table_name = table_name
        self.rows_imported = rows_imported
        super().__init__(message, **kwargs)
        self.context.update({'table_name': table_name, 'rows_imported': rows_imported})


class BatchError(ImportError_):
    """Erreur sur un batch lors de l'insertion."""

    def __init__(
        self,
        message: str,
        batch_num: Optional[int] = None,
        batch_size: Optional[int] = None,
        **kwargs,
    ):
        self.batch_num = batch_num
        self.batch_size = batch_size
        super().__init__(message, **kwargs)
        self.context.update({'batch_num': batch_num, 'batch_size': batch_size})


class DataError(ImportError_):
    """Erreur de données (format invalide, validation)."""

    def __init__(
        self,
        message: str,
        column_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        **kwargs,
    ):
        self.column_name = column_name
        self.invalid_value = invalid_value
        super().__init__(message, **kwargs)
        self.context.update({
            'column_name': column_name,
            'invalid_value': str(invalid_value) if invalid_value else None,
        })


class DatabaseError(ImportError_):
    """Erreur de base de données."""

    def __init__(
        self,
        message: str,
        sql_state: Optional[str] = None,
        is_connection_error: bool = False,
        **kwargs,
    ):
        self.sql_state = sql_state
        self.is_connection_error = is_connection_error
        super().__init__(message, **kwargs)
        self.context.update({
            'sql_state': sql_state,
            'is_connection_error': is_connection_error,
        })


class SchemaError(BaseError):
    """Erreur de schéma ou structure de table."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        **kwargs,
    ):
        self.table_name = table_name
        self.schema_name = schema_name
        super().__init__(message, **kwargs)
        self.context.update({'table_name': table_name, 'schema_name': schema_name})


class BlueGreenError(BaseError):
    """Erreur Blue/Green."""

    def __init__(
        self,
        message: str,
        active_schema: Optional[str] = None,
        target_schema: Optional[str] = None,
        **kwargs,
    ):
        self.active_schema = active_schema
        self.target_schema = target_schema
        super().__init__(message, **kwargs)
        self.context.update({
            'active_schema': active_schema,
            'target_schema': target_schema,
        })


class ConcurrentImportError(BlueGreenError):
    """Un import est déjà en cours."""

    def __init__(
        self,
        message: str = "Un import est déjà en cours",
        import_started_at: Optional[str] = None,
        **kwargs,
    ):
        self.import_started_at = import_started_at
        super().__init__(message, **kwargs)
        self.context['import_started_at'] = import_started_at


class ViewSwitchError(BlueGreenError):
    """Erreur lors du switch des vues Blue/Green."""

    def __init__(
        self,
        message: str,
        failed_view: Optional[str] = None,
        **kwargs,
    ):
        self.failed_view = failed_view
        super().__init__(message, **kwargs)
        self.context['failed_view'] = failed_view


__all__ = [
    'BaseError',
    'ImportError_',
    'BatchError',
    'DataError',
    'DatabaseError',
    'SchemaError',
    'BlueGreenError',
    'ConcurrentImportError',
    'ViewSwitchError',
]
