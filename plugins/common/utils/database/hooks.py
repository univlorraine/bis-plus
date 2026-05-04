"""
Factory et gestionnaire centralisé pour les hooks PostgreSQL.

Ce module est neutre : il ne dépend que d'Airflow et ne référence aucun
plugin (AMUE, ECC). Les hooks spécifiques à un plugin (ex: AMUEAPIHook)
sont fournis par leur propre factory côté plugin.

USAGE
    # Factory (nouvelle instance à chaque appel)
    from common.utils.database.hooks import create_postgres_hook
    pg_hook = create_postgres_hook()

    # Singleton thread-local
    from common.utils.database.hooks import HookManager
    pg_hook = HookManager().postgres_hook
"""
from __future__ import annotations
import threading

from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================================
# CONFIGURATION DES HOOKS
# ============================================================================

# Configuration par défaut pour PostgreSQL
POSTGRES_DEFAULT_CONN_ID = 'postgres_data'   # ID de connexion Airflow
POSTGRES_DEFAULT_SCHEMA = 'splus'             # Schéma PostgreSQL pour les données AMUE
POSTGRES_DEFAULT_OPTIONS = f'-c search_path={POSTGRES_DEFAULT_SCHEMA}'

# Schémas Blue/Green
SCHEMA_BLUE = 'splus_blue'
SCHEMA_GREEN = 'splus_green'


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def create_postgres_hook(
    conn_id: str = POSTGRES_DEFAULT_CONN_ID,
    schema: str = POSTGRES_DEFAULT_SCHEMA,
    bluegreen_schema: str = None
) -> PostgresHook:
    """
    Factory pour créer un hook PostgreSQL avec configuration standard

    Args:
        conn_id: ID de connexion Airflow (défaut: 'postgres_data')
        schema: Schéma PostgreSQL (défaut: 'splus')
        bluegreen_schema: Schéma blue/green spécifique (prioritaire sur schema)
            Valeurs: 'splus_blue', 'splus_green', ou None

    Returns:
        PostgresHook configuré

    Example:
        >>> hook = create_postgres_hook()
        >>> hook = create_postgres_hook(conn_id='autre_db', schema='public')
        >>> hook = create_postgres_hook(bluegreen_schema='splus_blue')
    """
    # Le schéma blue/green a priorité s'il est spécifié
    effective_schema = bluegreen_schema if bluegreen_schema else schema

    return PostgresHook(
        postgres_conn_id=conn_id,
        options=f'-c search_path={effective_schema}'
    )


def resolve_postgres_hook(
    hook: PostgresHook = None,
    target_schema: str = None,
) -> PostgresHook:
    """Retourne ``hook`` s'il est fourni, sinon en fabrique un nouveau.

    Args:
        hook:          Hook déjà construit (injecté en paramètre), ou None.
        target_schema: Schéma blue/green à utiliser si le hook doit être créé.
    """
    if hook is not None:
        return hook
    return create_postgres_hook(bluegreen_schema=target_schema)


def create_bluegreen_hook(target_schema: str) -> PostgresHook:
    """
    Factory pour créer un hook PostgreSQL pour un schéma blue/green spécifique.

    Args:
        target_schema: Schéma cible ('splus_blue' ou 'splus_green')

    Returns:
        PostgresHook configuré pour le schéma cible

    Example:
        >>> hook = create_bluegreen_hook('splus_blue')
    """
    if target_schema not in (SCHEMA_BLUE, SCHEMA_GREEN):
        raise ValueError(f"Schéma invalide: {target_schema}. Attendu: {SCHEMA_BLUE} ou {SCHEMA_GREEN}")

    return create_postgres_hook(bluegreen_schema=target_schema)


# ============================================================================
# SINGLETON MANAGER (pour réutilisation des connexions)
# ============================================================================

class HookManager:
    """
    Gestionnaire centralisé de hooks PostgreSQL (singleton par thread).

    Utilise threading.local() pour isoler les connexions par thread.
    Pour des connexions indépendantes, utiliser `create_postgres_hook()`.
    """

    _instance = None
    _local = threading.local()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def postgres_hook(self) -> PostgresHook:
        """Retourne le hook PostgreSQL — isolé par thread (lazy loading)."""
        if not hasattr(self._local, 'postgres_hook') or self._local.postgres_hook is None:
            self._local.postgres_hook = create_postgres_hook()
        return self._local.postgres_hook

    def reset(self) -> None:
        """Réinitialise le hook du thread courant (utile pour les tests)."""
        self._local.postgres_hook = None
