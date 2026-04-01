"""Exceptions d'import : AMUEImportError, AMUEBatchError, AMUEDataError, AMUEDatabaseError."""
from typing import Optional, Any

from amue.exceptions.base import AMUEError


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
