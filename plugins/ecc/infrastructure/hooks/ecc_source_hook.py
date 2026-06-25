# ecc/hooks/ecc_source_hook.py
"""Layer: infrastructure

Hook source ECC : lecture Oracle ou SQL Server.

Ne dépend ni de airflow.providers.oracle ni de airflow.providers.microsoft.mssql
(non installés). Se connecte directement via le driver Python natif :
- Oracle : oracledb (ou cx_Oracle en fallback)
- SQL Server : pyodbc (ODBC Driver 17/18 for SQL Server requis)

Le backend est déduit du champ `conn_type` de la connexion Airflow :
- 'oracle' → backend Oracle
- 'mssql' / 'mssqlplus' → backend SQL Server
- 'odbc' → lit `extra.backend` ('oracle' ou 'mssql', défaut 'oracle' pour
  rétro-compat avec l'ancienne connexion `oracle_data`)
"""
import json
import logging
import os
import time
from typing import Iterator, List, Tuple

from common.infrastructure.config.airflow_helpers import get_airflow_connection

logger = logging.getLogger(__name__)

ORACLE = 'oracle'
MSSQL = 'mssql'

_DEFAULT_MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server'
_DEFAULT_MSSQL_PORT = 1433
_DEFAULT_ORACLE_PORT = 1521


def _get_driver(backend: str):
    """Importe et retourne le module driver pour le backend demandé."""
    if backend == ORACLE:
        try:
            import oracledb
            return oracledb
        except ImportError:
            pass
        try:
            import cx_Oracle
            return cx_Oracle
        except ImportError:
            raise ImportError(
                "Aucun driver Oracle trouvé. Installez 'oracledb' ou 'cx_Oracle'."
            )
    if backend == MSSQL:
        try:
            import pyodbc
            return pyodbc
        except ImportError:
            raise ImportError(
                "Driver pyodbc non installé pour SQL Server. "
                "Installez 'pyodbc' et le pilote 'ODBC Driver 17/18 for SQL Server'."
            )
    raise ValueError(
        f"Backend non supporté: {backend!r} (attendu: 'oracle' ou 'mssql')"
    )


def _parse_extra(conn) -> dict:
    """Parse le champ extra Airflow (str JSON ou dict) en dict, sans planter."""
    extra = getattr(conn, 'extra', None)
    if not extra:
        return {}
    if isinstance(extra, dict):
        return extra
    try:
        return json.loads(extra)
    except (TypeError, ValueError):
        return {}


class ECCSourceHook:
    """
    Hook de lecture des données SAP ECC depuis Oracle ou SQL Server.

    Le backend est résolu automatiquement à partir du `conn_type` Airflow.
    Le champ `schema` porte le SID Oracle ou le nom de base SQL Server.

    Example:
        >>> hook = ECCSourceHook()  # conn_id='ecc_data' par défaut
        >>> columns, rows = hook.execute_sql_file('/path/to/SELECT_LFA1.sql')
        >>> for row in rows:
        ...     print(row)
    """

    def __init__(self, conn_id: str = 'ecc_data'):
        """
        Args:
            conn_id: ID de connexion Airflow (défaut: 'ecc_data')
        """
        self.conn_id = conn_id

    def _resolve_backend(self, conn) -> str:
        """Déduit le backend ('oracle' ou 'mssql') depuis la connexion Airflow."""
        conn_type = (getattr(conn, 'conn_type', None) or '').lower()
        if conn_type == ORACLE:
            return ORACLE
        if conn_type in ('mssql', 'mssqlplus'):
            return MSSQL
        if conn_type == 'odbc':
            extra = _parse_extra(conn)
            backend = (extra.get('backend') or ORACLE).lower()
            if backend not in (ORACLE, MSSQL):
                raise ValueError(
                    f"[ECC] extra.backend={backend!r} invalide (attendu 'oracle' ou 'mssql')"
                )
            return backend
        raise ValueError(
            f"[ECC] conn_type Airflow non supporté: {conn_type!r} "
            f"(attendu 'oracle', 'mssql', 'mssqlplus' ou 'odbc')"
        )

    def _build_connect_kwargs(self, backend: str, driver, conn) -> dict:
        """Construit les kwargs de connexion natifs selon le backend."""
        host = conn.host or 'localhost'
        login = conn.login or ''
        password = conn.password or ''
        schema = conn.schema or ''

        if backend == ORACLE:
            port = conn.port or _DEFAULT_ORACLE_PORT
            dsn = driver.makedsn(host, port, sid=schema)
            return {
                'user': login,
                'password': password,
                'dsn': dsn,
                'expire_time': 2,
            }

        # MSSQL — chaîne de connexion ODBC complète
        port = conn.port or _DEFAULT_MSSQL_PORT
        extra = _parse_extra(conn)
        odbc_driver = extra.get('driver') or _DEFAULT_MSSQL_DRIVER
        # TrustServerCertificate=yes désactive la validation du certificat serveur (MITM possible).
        # En production (AIRFLOW_ENV=prod), passer "no" et s'assurer que le serveur a un cert valide.
        _env = os.environ.get("AIRFLOW_ENV", "dev").lower()
        trust_cert = "yes" if _env == "dev" else "no"
        dsn = (
            f"Driver={{{odbc_driver}}};"
            f"Server={host},{port};"
            f"Database={schema};"
            f"UID={login};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate={trust_cert}"
        )
        return {'dsn': dsn}

    @staticmethod
    def _connect(driver, backend: str, kwargs: dict):
        """Appelle driver.connect() avec la signature attendue par chaque backend."""
        if backend == ORACLE:
            return driver.connect(**kwargs)
        # pyodbc.connect prend une chaîne unique
        return driver.connect(kwargs['dsn'])

    def get_conn(self, max_retries: int = 3, retry_delay_seconds: float = 5.0):
        """
        Retourne une connexion native au backend ECC, avec retry automatique.

        Args:
            max_retries: Nombre maximum de tentatives (défaut: 3)
            retry_delay_seconds: Délai entre tentatives en secondes (défaut: 5)

        Returns:
            Connexion native (oracledb / cx_Oracle / pyodbc) active

        Raises:
            Exception: Dernière exception du driver si toutes les tentatives échouent
        """
        airflow_conn = get_airflow_connection(self.conn_id)
        backend = self._resolve_backend(airflow_conn)
        driver = _get_driver(backend)
        connect_kwargs = self._build_connect_kwargs(backend, driver, airflow_conn)

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "[ECC] Tentative connexion %s %d/%d (conn_id=%s)",
                    backend.upper(), attempt, max_retries, self.conn_id,
                )
                logger.debug(
                    "[ECC] Détail connexion %s: %s@%s (db=%s)",
                    backend.upper(), airflow_conn.login, airflow_conn.host, airflow_conn.schema,
                )
                return self._connect(driver, backend, connect_kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(
                        f"[ECC] Échec connexion {backend.upper()} "
                        f"(tentative {attempt}/{max_retries}): {e}"
                    )
                    time.sleep(retry_delay_seconds)
                else:
                    logger.error(
                        f"[ECC] Toutes les tentatives de connexion {backend.upper()} "
                        f"ont échoué: {e}"
                    )

        raise last_error

    def _current_backend(self) -> str:
        """Helper utilisé par _stream_query pour adapter la détection d'erreur."""
        return self._resolve_backend(get_airflow_connection(self.conn_id))

    @staticmethod
    def _is_connection_error(e: Exception, backend: str) -> bool:
        """Détecte les erreurs de transport récupérables selon le backend."""
        msg = str(e)
        if backend == ORACLE:
            return 'DPY-4011' in msg or 'connection was closed' in msg.lower()
        # MSSQL / pyodbc — SQLSTATE de transport
        for sqlstate in ('08S01', '08001', '08003', '08004', 'HYT00', 'HYT01'):
            if sqlstate in msg:
                return True
        return 'connection' in msg.lower() and 'closed' in msg.lower()

    def _stream_query(
        self,
        sql_query: str,
        batch_size: int,
    ) -> Tuple[List[str], Iterator[tuple]]:
        """Logique partagée : exécute la requête, renvoie (colonnes, générateur)."""
        backend = self._current_backend()
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_query)

        column_names = [desc[0].lower() for desc in cursor.description]
        logger.info(f"[ECC] {len(column_names)} colonnes: {column_names[:5]}...")

        def row_generator():
            nonlocal conn, cursor
            fetched = 0
            max_reconnects = 2
            reconnects = 0
            try:
                while True:
                    try:
                        rows = cursor.fetchmany(batch_size)
                    except Exception as e:
                        if reconnects < max_reconnects and self._is_connection_error(e, backend):
                            logger.warning(
                                f"[ECC] Connexion {backend.upper()} perdue à {fetched} lignes, "
                                f"reconnexion {reconnects + 1}/{max_reconnects}..."
                            )
                            try:
                                cursor.close()
                            except Exception as _e:
                                logger.warning(f"[ECC] Échec fermeture curseur lors reconnexion: {_e}")
                            try:
                                conn.close()
                            except Exception as _e:
                                logger.warning(f"[ECC] Échec fermeture connexion lors reconnexion: {_e}")
                            reconnects += 1
                            conn = self.get_conn()
                            cursor = conn.cursor()
                            cursor.execute(sql_query)
                            fetched = 0
                            continue
                        raise
                    if not rows:
                        break
                    fetched += len(rows)
                    for row in rows:
                        yield row
            finally:
                logger.info(f"[ECC] Total lignes {backend.upper()} récupérées: {fetched}")
                try:
                    cursor.close()
                except Exception as _e:
                    logger.warning(f"[ECC] Échec fermeture curseur: {_e}")
                try:
                    conn.close()
                except Exception as _e:
                    logger.warning(f"[ECC] Échec fermeture connexion: {_e}")

        return column_names, row_generator()

    def execute_sql_file(
        self,
        sql_file_path: str,
        batch_size: int = 5000,
    ) -> Tuple[List[str], Iterator[tuple]]:
        """
        Exécute un fichier SQL et retourne un générateur de lignes.

        Args:
            sql_file_path: Chemin absolu vers le fichier SQL
            batch_size: Nombre de lignes par fetch (défaut: 5000)

        Returns:
            Tuple (column_names_lowercase, row_generator)

        Raises:
            FileNotFoundError: Si le fichier SQL n'existe pas
        """
        logger.info(f"[ECC] Lecture fichier SQL: {sql_file_path}")
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read().strip().rstrip(';').strip()
        return self._stream_query(sql_query, batch_size)

    def execute_query(
        self,
        sql_query: str,
        batch_size: int = 5000,
    ) -> Tuple[List[str], Iterator[tuple]]:
        """
        Exécute une requête SQL passée en chaîne (depuis amue_tables.ecc_query).

        Args:
            sql_query: Requête SQL (depuis splus_admin.amue_tables.ecc_query)
            batch_size: Nombre de lignes par fetch (défaut: 5000)

        Returns:
            Tuple (column_names_lowercase, row_generator)

        Raises:
            ValueError: Si sql_query est vide ou None
        """
        if not sql_query or not sql_query.strip():
            raise ValueError("[ECC] execute_query: sql_query vide ou None")

        query = sql_query.strip().rstrip(';').strip()
        logger.info(
            f"[ECC] Exécution requête (depuis base de données) — "
            f"aperçu: {query[:20000]!r}"
        )
        return self._stream_query(query, batch_size)
