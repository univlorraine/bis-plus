"""Layer: domain

Abstractions dont dépend la logique métier (application). Les modules
d'infrastructure (hooks Postgres, connection manager...) implémentent ces
contrats de manière purement structurelle (Protocol), sans héritage requis
et sans framework d'injection de dépendances.

Aucun import airflow/psycopg2 n'est autorisé dans ce module.
"""
from typing import Any, Optional, Protocol, Sequence


class SqlExecutor(Protocol):
    """Sous-ensemble de l'API de PostgresHook utilisé par la logique métier."""

    def run(self, sql: str, parameters: Optional[Sequence] = None) -> None: ...

    def get_records(self, sql: str, parameters: Optional[Sequence] = None) -> list: ...

    def get_first(self, sql: str, parameters: Optional[Sequence] = None) -> Optional[tuple]: ...

    def get_conn(self) -> Any: ...


class ConnectionProvider(Protocol):
    """Cycle de vie d'une connexion DB-API (déjà la surface de PostgresConnectionManager)."""

    def get_connection(self) -> Any: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class StateStore(Protocol):
    """Persistance de l'état blue/green (déjà la surface publique d'AdminStateManager)."""

    def get_bluegreen_state(self) -> Any: ...

    def save_bluegreen_state(self, state: Any) -> bool: ...

    def try_acquire_import_lock(self, started_at: str, correlation_id: str) -> bool: ...

    def release_import_lock(self, active_schema: str) -> bool: ...
