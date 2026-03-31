"""
Gestionnaire centralisé des connexions PostgreSQL.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module fournit un gestionnaire de connexions PostgreSQL réutilisable
qui centralise la logique de gestion des connexions, évitant la duplication
dans les différents opérateurs.

USAGE :
    >>> from common.utils.database.connection_manager import PostgresConnectionManager
    >>> with PostgresConnectionManager(postgres_hook) as manager:
    ...     conn = manager.get_connection()
    ...     cursor = conn.cursor()
    ...     # utilisation...

================================================================================
"""
import logging
from typing import Optional, Any

from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


class PostgresConnectionManager:
    """
    Gestionnaire centralisé des connexions PostgreSQL.

    Cette classe encapsule la gestion du cycle de vie des connexions
    PostgreSQL, incluant :
    - Création de connexion paresseuse (lazy)
    - Réutilisation de connexion existante
    - Fermeture propre des connexions
    - Support du context manager (with statement)

    Attributes:
        hook: Hook PostgreSQL Airflow pour la création de connexions

    Example:
        >>> from airflow.providers.postgres.hooks.postgres import PostgresHook
        >>> hook = PostgresHook('postgres_data')
        >>> manager = PostgresConnectionManager(hook)
        >>> conn = manager.get_connection()
        >>> # ... utilisation de la connexion ...
        >>> manager.close()

        # Ou avec context manager :
        >>> with PostgresConnectionManager(hook) as manager:
        ...     conn = manager.get_connection()
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT 1")
    """

    def __init__(self, postgres_hook: Any = None):
        """
        Initialise le gestionnaire de connexions.

        Args:
            postgres_hook: Instance de PostgresHook Airflow.
                          Si None, doit être défini avant l'utilisation.
        """
        self._hook = postgres_hook
        self._conn: Optional[Any] = None

    @property
    def hook(self) -> Any:
        """Retourne le hook PostgreSQL."""
        return self._hook

    @hook.setter
    def hook(self, value: Any) -> None:
        """Définit le hook PostgreSQL. Ferme la connexion existante."""
        if self._conn is not None:
            self.close()
        self._hook = value

    @property
    def is_connected(self) -> bool:
        """Vérifie si une connexion active existe."""
        return self._conn is not None and not getattr(self._conn, 'closed', True)

    def get_connection(self) -> Any:
        """
        Retourne une connexion PostgreSQL réutilisable.

        Crée une nouvelle connexion si aucune n'existe ou si la
        connexion existante est fermée.

        Returns:
            Connexion psycopg2 active

        Raises:
            AirflowException: Si le hook n'est pas configuré
        """
        if self._conn is None or getattr(self._conn, 'closed', True):
            if self._hook is None:
                raise AirflowException("PostgreSQL hook non configuré")
            self._conn = self._hook.get_conn()
            logger.debug("[CONN_MGR] Nouvelle connexion PostgreSQL créée")
        return self._conn

    def close(self) -> None:
        """
        Ferme proprement la connexion PostgreSQL.

        Gère silencieusement les cas où la connexion est déjà fermée
        ou n'existe pas.
        """
        if self._conn is not None:
            try:
                if not getattr(self._conn, 'closed', True):
                    self._conn.close()
                    logger.debug("[CONN_MGR] Connexion PostgreSQL fermée")
            except Exception as e:
                logger.warning(f"[CONN_MGR] Erreur lors de la fermeture: {e}")
            finally:
                self._conn = None

    def rollback(self) -> None:
        """
        Effectue un rollback sur la connexion courante.

        Gère silencieusement les cas où la connexion n'existe pas
        ou est déjà fermée.
        """
        if self._conn is not None and not getattr(self._conn, 'closed', True):
            try:
                self._conn.rollback()
                logger.debug("[CONN_MGR] Rollback effectué")
            except Exception as e:
                logger.warning(f"[CONN_MGR] Erreur lors du rollback: {e}")

    def commit(self) -> None:
        """
        Effectue un commit sur la connexion courante.

        Raises:
            AirflowException: Si aucune connexion n'est active
        """
        if self._conn is None or getattr(self._conn, 'closed', True):
            raise AirflowException("Pas de connexion active pour commit")
        self._conn.commit()
        logger.debug("[CONN_MGR] Commit effectué")

    def __enter__(self) -> 'PostgresConnectionManager':
        """Support du context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """
        Fermeture automatique en sortie du context manager.

        En cas d'exception, effectue un rollback avant de fermer.
        """
        if exc_type is not None:
            self.rollback()
        self.close()
        return False  # Ne supprime pas l'exception

    def __repr__(self) -> str:
        """Représentation string de l'instance."""
        status = "connected" if self.is_connected else "disconnected"
        return f"PostgresConnectionManager(status={status})"
