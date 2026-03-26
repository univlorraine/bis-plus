"""
Types et structures de données pour le module AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module définit les TypedDict et types utilisés dans le projet AMUE.
Il fournit une typage statique fort pour les structures de données
courantes comme les informations de tables, les résultats d'import, etc.

USAGE :
    from amue.types_amue import TableInfo, ImportResult, ColumnInfo

    def process_table(table: TableInfo) -> ImportResult:
        ...

================================================================================
"""
from typing import TypedDict, List, Optional, Literal


class ColumnInfo(TypedDict):
    """
    Informations sur une colonne de table.

    Attributes:
        name: Nom de la colonne (en minuscules)
        type_original: Type original depuis l'API (ex: 'CHAR(4)')
        type_postgres: Type PostgreSQL mappé (ex: 'VARCHAR(4)')
    """
    name: str
    type_original: str
    type_postgres: str


class TableInfo(TypedDict):
    """
    Informations sur une table à importer.

    Attributes:
        name: Nom de la table (ex: 'CSKS')
        columns: Liste des informations de colonnes
        primary_key: Clé primaire (colonnes séparées par virgule)
        enabled: Si la table est activée pour l'import
        delta: Nom de la colonne de date pour import différentiel (optionnel)
    """
    name: str
    columns: List[ColumnInfo]
    primary_key: str
    enabled: bool
    delta: Optional[str]


class TableInfoPartial(TypedDict, total=False):
    """
    Version partielle de TableInfo pour les mises à jour.
    Tous les champs sont optionnels.
    """
    name: str
    columns: List[ColumnInfo]
    primary_key: str
    enabled: bool
    delta: Optional[str]


class ImportResult(TypedDict, total=False):
    """
    Résultat d'une opération d'import.

    Attributes:
        table_name: Nom de la table importée
        rows_inserted: Nombre de nouvelles lignes insérées (INSERT)
        rows_updated: Nombre de lignes existantes mises à jour (UPDATE)
        rows_fetched: Nombre de lignes récupérées depuis l'API
        import_type: Type d'import ('full' ou 'delta')
        status: Statut de l'import ('success' ou 'error')
        correlation_id: ID de corrélation pour le tracing
        fingerprint_API: Empreinte de structure originale API
        fingerprint_UL: Empreinte de structure transformée PG
        table_finish: Date finish de la table côté API AMUE
        batch_count: Nombre de batches traités
        total_duration_seconds: Durée totale d'insertion en secondes
        avg_batch_duration: Durée moyenne par batch en secondes
    """
    table_name: str
    rows_inserted: int
    rows_updated: int
    rows_fetched: int
    import_type: str
    status: str
    correlation_id: str
    fingerprint_API: str
    fingerprint_UL: str
    table_finish: str
    batch_count: int
    total_duration_seconds: float
    avg_batch_duration: float


class ImportResultPartial(TypedDict, total=False):
    """Version partielle d'ImportResult pour les créations (compat)."""
    table_name: str
    rows_inserted: int
    rows_fetched: int
    import_type: str
    status: str
    correlation_id: str
    fingerprint_API: str
    fingerprint_UL: str
    batch_count: int
    total_duration_seconds: float
    avg_batch_duration: float


class BlueGreenStateDict(TypedDict):
    """
    État du déploiement blue/green sous forme de dictionnaire.

    Attributes:
        last_import_schema: Dernier schéma où un import a été effectué
        last_switch_timestamp: Timestamp ISO du dernier switch
        last_sync_timestamp: Timestamp ISO de la dernière synchronisation
        import_in_progress: True si un import est en cours
        import_started_at: Timestamp ISO du début de l'import en cours
        import_correlation_id: ID de corrélation de l'import en cours
    """
    last_import_schema: str
    last_switch_timestamp: str
    last_sync_timestamp: str
    import_in_progress: bool
    import_started_at: str
    import_correlation_id: str


class TableManagementResultDict(TypedDict):
    """
    Résultat d'une opération de gestion de table.

    Attributes:
        table_name: Nom de la table gérée
        columns: Liste des noms de colonnes
        primary_keys: Clés primaires (format CSV)
        created: True si la table a été créée
        status: Statut de l'opération ('success' ou 'error')
        error: Message d'erreur si status='error'
    """
    table_name: str
    columns: List[str]
    primary_keys: str
    created: bool
    status: str
    error: Optional[str]


class StructureInfo(TypedDict):
    """
    Informations de structure d'une table pour le TableManager.

    Attributes:
        table_name: Nom de la table
        columns: Liste des définitions de colonnes
        primary_keys: Clés primaires (format CSV)
        exists: True si la table existe déjà
    """
    table_name: str
    columns: List[ColumnInfo]
    primary_keys: str
    exists: bool


class ImportConfig(TypedDict, total=False):
    """
    Configuration pour un import de table.

    Attributes:
        import_type: Type d'import ('full' ou 'delta')
        delta: Nom de la colonne de date pour le différentiel
        last_import: Date ISO de référence pour le filtrage différentiel (depuis amue_state.last_report_start)
        fingerprint_API: Empreinte de structure originale API
        fingerprint_UL: Empreinte de structure transformée PG
        table_finish: Date finish de la table côté API AMUE
    """
    import_type: Literal['full', 'delta']
    delta: Optional[str]
    last_import: Optional[str]
    fingerprint_API: Optional[str]
    fingerprint_UL: Optional[str]
    table_finish: Optional[str]


class BatchResult(TypedDict):
    """
    Résultat d'une opération de batch.

    Attributes:
        batch_num: Numéro du batch
        rows_processed: Nombre de lignes traitées
        success: True si le batch a réussi
        error: Message d'erreur si échec
    """
    batch_num: int
    rows_processed: int
    success: bool
    error: Optional[str]


class LockInfo(TypedDict):
    """
    Informations sur un verrou d'import.

    Attributes:
        import_in_progress: True si un import est en cours
        import_started_at: Timestamp ISO du début
        import_correlation_id: ID de corrélation
        is_stale: True si le verrou est considéré comme abandonné
        target_schema: Schéma cible de l'import
    """
    import_in_progress: bool
    import_started_at: str
    import_correlation_id: str
    is_stale: bool
    target_schema: str


# Type aliases pour plus de clarté
TableName = str
SchemaName = str
ColumnName = str
PrimaryKeyList = List[str]
CorrelationId = str
