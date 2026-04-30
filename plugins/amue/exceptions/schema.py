"""Exceptions de schéma/structure : AMUESchemaError, AMUETableNotFoundError,
AMUEStructureChangedError, TableNotFoundError."""
from typing import List, Optional

from airflow.exceptions import AirflowException

from common.exceptions import SchemaError

from amue.exceptions.base import AMUEError


class AMUESchemaError(SchemaError, AMUEError):
    """Erreur liée au schéma ou à la structure des tables AMUE."""
    pass


class AMUETableNotFoundError(AMUESchemaError):
    """Table non trouvée dans la base de données."""
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
        **kwargs,
    ):
        self.old_fingerprint = old_fingerprint
        self.new_fingerprint = new_fingerprint
        self.changes = changes
        super().__init__(message, **kwargs)
        self.context.update({
            'old_fingerprint': old_fingerprint,
            'new_fingerprint': new_fingerprint,
            'changes': changes,
        })


class TableNotFoundError(AirflowException):
    """
    Exception levée quand une table configurée n'est pas trouvée dans le statut API.

    Cette exception est CRITIQUE : elle indique une incohérence entre la
    configuration Airflow et les données disponibles côté AMUE.

    Attributes:
        missing_tables: Liste des noms de tables manquantes
        configured_count: Nombre total de tables configurées
        found_count: Nombre de tables trouvées dans l'API
    """

    def __init__(self, missing_tables: List[str], configured_count: int, found_count: int):
        self.missing_tables = missing_tables
        self.configured_count = configured_count
        self.found_count = found_count

        message = (
            f"ERREUR CRITIQUE: {len(missing_tables)} table(s) configurée(s) "
            f"absente(s) du statut API.\n"
            f"Tables configurées: {configured_count}\n"
            f"Tables trouvées: {found_count}\n"
            f"Tables manquantes: {', '.join(missing_tables)}"
        )

        super().__init__(message)
