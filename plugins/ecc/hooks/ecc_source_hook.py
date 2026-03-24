# ecc/hooks/ecc_source_hook.py
"""Hook Oracle pour la lecture des données SAP ECC.

N'utilise pas airflow.providers.oracle (non installé) mais se connecte
directement via oracledb (ou cx_Oracle en fallback) en lisant les
métadonnées de la connexion Airflow 'ecc_data'.
"""
import logging
import time
from typing import Iterator, List, Tuple

from amue.utils.config.airflow_helpers import get_airflow_connection

logger = logging.getLogger(__name__)


def _get_oracle_driver():
    """Retourne le module Oracle disponible (oracledb ou cx_Oracle)."""
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


class ECCSourceHook:
    """
    Hook de lecture Oracle pour les données SAP ECC.

    Se connecte via oracledb/cx_Oracle en utilisant les paramètres de la
    connexion Airflow (conn_id). N'utilise pas airflow.providers.oracle.

    Example:
        >>> hook = ECCSourceHook()
        >>> columns, rows = hook.execute_sql_file('/path/to/SELECT_LFA1.sql')
        >>> for row in rows:
        ...     print(row)
    """

    def __init__(self, conn_id: str = 'oracle_data'):
        """
        Initialise le hook Oracle.

        Args:
            conn_id: ID de connexion Airflow (défaut: 'oracle_data')
        """
        self.conn_id = conn_id

    def _build_dsn(self, oracle, conn) -> str:
        """Construit le DSN Oracle (format SID) depuis les métadonnées Airflow.

        Le champ 'schema' de la connexion Airflow contient le SID Oracle.
        """
        host = conn.host or 'localhost'
        port = conn.port or 1521
        sid = conn.schema or ''
        # Format TNS descriptor avec SID (≠ service_name)
        return oracle.makedsn(host, port, sid=sid)

    def get_conn(self, max_retries: int = 3, retry_delay_seconds: float = 5.0):
        """
        Retourne une connexion Oracle native avec retry automatique.

        Args:
            max_retries: Nombre maximum de tentatives (défaut: 3)
            retry_delay_seconds: Délai entre tentatives en secondes (défaut: 5)

        Returns:
            Connexion Oracle active

        Raises:
            Exception: Dernière exception Oracle si toutes les tentatives échouent
        """
        oracle = _get_oracle_driver()
        airflow_conn = get_airflow_connection(self.conn_id)

        dsn = self._build_dsn(oracle, airflow_conn)
        login = airflow_conn.login or ''
        password = airflow_conn.password or ''

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"[ECC] Connexion Oracle: {login}@{airflow_conn.host} "
                    f"(SID={airflow_conn.schema}, tentative {attempt}/{max_retries})"
                )
                return oracle.connect(user=login, password=password, dsn=dsn, expire_time=2)
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(
                        f"[ECC] Échec connexion Oracle (tentative {attempt}/{max_retries}): {e}"
                    )
                    time.sleep(retry_delay_seconds)
                else:
                    logger.error(f"[ECC] Toutes les tentatives de connexion Oracle ont échoué: {e}")

        raise last_error

    @staticmethod
    def _is_connection_error(e: Exception) -> bool:
        """Détecte les erreurs de connexion Oracle récupérables (DPY-4011, etc.)."""
        msg = str(e)
        return 'DPY-4011' in msg or 'connection was closed' in msg.lower()

    def execute_sql_file(
        self,
        sql_file_path: str,
        batch_size: int = 5000
    ) -> Tuple[List[str], Iterator[tuple]]:
        """
        Exécute un fichier SQL Oracle et retourne un générateur de lignes.

        Args:
            sql_file_path: Chemin absolu vers le fichier SQL
            batch_size: Nombre de lignes par fetch (défaut: 5000)

        Returns:
            Tuple (column_names_lowercase, row_generator) :
            - column_names_lowercase : noms de colonnes en minuscules
            - row_generator : générateur de tuples (une ligne par itération)

        Raises:
            FileNotFoundError: Si le fichier SQL n'existe pas
        """
        logger.info(f"[ECC] Lecture fichier SQL: {sql_file_path}")

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_query = f.read().strip().rstrip(';').strip()

        conn = self.get_conn()
        cursor = conn.cursor()

        logger.info("[ECC] Exécution requête Oracle")
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
                        if reconnects < max_reconnects and self._is_connection_error(e):
                            logger.warning(
                                f"[ECC] Connexion Oracle perdue à {fetched} lignes "
                                f"(DPY-4011), reconnexion {reconnects + 1}/{max_reconnects}..."
                            )
                            try: cursor.close()
                            except Exception: pass
                            try: conn.close()
                            except Exception: pass
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
                logger.info(f"[ECC] Total lignes Oracle récupérées: {fetched}")
                try: cursor.close()
                except Exception: pass
                try: conn.close()
                except Exception: pass

        return column_names, row_generator()

    def execute_query(
        self,
        sql_query: str,
        batch_size: int = 5000
    ) -> Tuple[List[str], Iterator[tuple]]:
        """
        Exécute une requête Oracle passée en chaîne (depuis amue_tables.ecc_query).

        Args:
            sql_query: Requête Oracle SQL (depuis splus_admin.amue_tables.ecc_query)
            batch_size: Nombre de lignes par fetch (défaut: 5000)

        Returns:
            Tuple (column_names_lowercase, row_generator)

        Raises:
            ValueError: Si sql_query est vide ou None
        """
        if not sql_query or not sql_query.strip():
            raise ValueError("[ECC] execute_query: sql_query vide ou None")

        query = sql_query.strip().rstrip(';').strip()
        logger.info("[ECC] Exécution requête Oracle (depuis base de données)")

        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)

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
                        if reconnects < max_reconnects and self._is_connection_error(e):
                            logger.warning(
                                f"[ECC] Connexion Oracle perdue à {fetched} lignes "
                                f"(DPY-4011), reconnexion {reconnects + 1}/{max_reconnects}..."
                            )
                            try: cursor.close()
                            except Exception: pass
                            try: conn.close()
                            except Exception: pass
                            reconnects += 1
                            conn = self.get_conn()
                            cursor = conn.cursor()
                            cursor.execute(query)
                            fetched = 0
                            continue
                        raise
                    if not rows:
                        break
                    fetched += len(rows)
                    for row in rows:
                        yield row
            finally:
                logger.info(f"[ECC] Total lignes Oracle récupérées: {fetched}")
                try: cursor.close()
                except Exception: pass
                try: conn.close()
                except Exception: pass

        return column_names, row_generator()
