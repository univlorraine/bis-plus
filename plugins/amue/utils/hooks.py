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
    from amue.utils.hooks import create_postgres_hook, create_api_hook

    pg_hook = create_postgres_hook()
    api_hook = create_api_hook()

    # Singleton (même instance réutilisée)
    from amue.utils.hooks import HookManager

    manager = HookManager()
    pg_hook = manager.postgres_hook
    api_hook = manager.api_hook

================================================================================
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.hooks.amue_api_hook import AMUEAPIHook


# ============================================================================
# CONFIGURATION DES HOOKS
# ============================================================================

# Configuration par défaut pour PostgreSQL
POSTGRES_DEFAULT_CONN_ID = 'postgres_data'   # ID de connexion Airflow
POSTGRES_DEFAULT_SCHEMA = 'splus'             # Schéma PostgreSQL pour les données AMUE
POSTGRES_DEFAULT_OPTIONS = f'-c search_path={POSTGRES_DEFAULT_SCHEMA}'


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def create_postgres_hook(
    conn_id: str = POSTGRES_DEFAULT_CONN_ID,
    schema: str = POSTGRES_DEFAULT_SCHEMA
) -> PostgresHook:
    """
    Factory pour créer un hook PostgreSQL avec configuration standard

    Args:
        conn_id: ID de connexion Airflow (défaut: 'postgres_data')
        schema: Schéma PostgreSQL (défaut: 'splus')

    Returns:
        PostgresHook configuré

    Example:
        >>> hook = create_postgres_hook()
        >>> hook = create_postgres_hook(conn_id='autre_db', schema='public')
    """
    return PostgresHook(
        postgres_conn_id=conn_id,
        options=f'-c search_path={schema}'
    )


def create_api_hook() -> AMUEAPIHook:
    """
    Factory pour créer un hook API AMUE

    Returns:
        AMUEAPIHook configuré
    """
    return AMUEAPIHook()


# ============================================================================
# SINGLETON MANAGER (pour réutilisation des connexions)
# ============================================================================

class HookManager:
    """
    Gestionnaire centralisé de hooks (singleton)

    Utilise le pattern singleton pour réutiliser les connexions.
    Pour des connexions indépendantes, utiliser les factory functions.

    Example:
        >>> manager = HookManager()
        >>> api = manager.api_hook
        >>> pg = manager.postgres_hook
    """

    _instance = None
    _api_hook = None
    _postgres_hook = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def api_hook(self) -> AMUEAPIHook:
        """Retourne le hook API AMUE (lazy loading)"""
        if self._api_hook is None:
            self._api_hook = create_api_hook()
        return self._api_hook

    @property
    def postgres_hook(self) -> PostgresHook:
        """Retourne le hook PostgreSQL (lazy loading)"""
        if self._postgres_hook is None:
            self._postgres_hook = create_postgres_hook()
        return self._postgres_hook

    def reset(self) -> None:
        """Réinitialise les hooks (utile pour les tests)"""
        self._api_hook = None
        self._postgres_hook = None
