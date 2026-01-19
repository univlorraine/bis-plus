from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.hooks.amue_api_hook import AMUEAPIHook


# ============================================================================
# CONFIGURATION DES HOOKS
# ============================================================================

# Configuration par défaut pour PostgreSQL
POSTGRES_DEFAULT_CONN_ID = 'postgres_data'
POSTGRES_DEFAULT_SCHEMA = 'splus'
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
