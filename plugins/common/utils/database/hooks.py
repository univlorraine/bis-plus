from __future__ import annotations
"""
Factory et gestionnaire centralisé pour les hooks Airflow.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module centralise la création des hooks (connexions) utilisés par le projet.
Il offre deux approches :

1. FACTORY FUNCTIONS (recommandé) :
   Créent de nouvelles instances de hooks à chaque appel.
   Idéal pour les workers Airflow où chaque task a sa propre connexion.

2. SINGLETON (HookManager) :
   Réutilise les mêmes instances de hooks.
   Utile pour les tests ou les scripts où on veut éviter les connexions multiples.

================================================================================
POURQUOI CENTRALISER ?
================================================================================

1. Configuration unique : Les paramètres (conn_id, schema) sont définis ici
2. Changement facile : Modifier la connexion à un seul endroit
3. Testabilité : Facile de mocker les hooks pour les tests
4. Cohérence : Tous les composants utilisent la même configuration

================================================================================
CONNEXIONS AIRFLOW
================================================================================

Ce module utilise les connexions Airflow suivantes :

PostgreSQL ('postgres_data') :
    - Host : Serveur PostgreSQL
    - Schema : splus (schéma AMUE)
    - Login/Password : Credentials DB

API AMUE ('oauth_api') :
    - Login : Client ID OAuth
    - Password : Client Secret OAuth
    - Extra : {"token_url": "...", "api_base_url": "..."}

================================================================================
USAGE
================================================================================

    # Factory (nouvelle instance à chaque appel)
    from common.utils.database.hooks import create_postgres_hook, create_api_hook

    pg_hook = create_postgres_hook()
    api_hook = create_api_hook()

    # Singleton (même instance réutilisée)
    from common.utils.database.hooks import HookManager

    manager = HookManager()
    pg_hook = manager.postgres_hook
    api_hook = manager.api_hook

================================================================================
"""
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


def create_api_hook():
    """
    Factory pour créer un hook API AMUE.

    Note: Cette factory est le seul point où common importe d'amue (lazy).
    Elle n'est appelée qu'en contexte AMUE — common reste utilisable sans
    amue installé tant que ni cette factory ni `HookManager.api_hook` ne
    sont sollicités.

    Returns:
        AMUEAPIHook configuré
    """
    from amue.hooks.amue_api_hook import AMUEAPIHook
    return AMUEAPIHook()


# ============================================================================
# SINGLETON MANAGER (pour réutilisation des connexions)
# ============================================================================

class HookManager:
    """
    Gestionnaire centralisé de hooks (singleton par thread)

    Utilise threading.local() pour isoler les connexions par thread :
    chaque worker Airflow (LocalExecutor) obtient son propre hook,
    évitant le partage de connexions PostgreSQL entre threads.

    Pour des connexions indépendantes, utiliser les factory functions.

    Example:
        >>> manager = HookManager()
        >>> api = manager.api_hook
        >>> pg = manager.postgres_hook
    """

    _instance = None
    _local = threading.local()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def api_hook(self):
        """Retourne le hook API AMUE — isolé par thread (lazy loading)"""
        if not hasattr(self._local, 'api_hook') or self._local.api_hook is None:
            self._local.api_hook = create_api_hook()
        return self._local.api_hook

    @property
    def postgres_hook(self) -> PostgresHook:
        """Retourne le hook PostgreSQL — isolé par thread (lazy loading)"""
        if not hasattr(self._local, 'postgres_hook') or self._local.postgres_hook is None:
            self._local.postgres_hook = create_postgres_hook()
        return self._local.postgres_hook

    def reset(self) -> None:
        """Réinitialise les hooks du thread courant (utile pour les tests)"""
        self._local.api_hook = None
        self._local.postgres_hook = None
