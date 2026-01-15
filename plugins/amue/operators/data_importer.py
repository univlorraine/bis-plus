"""
Gestionnaire d'import des données depuis l'API AMUE
Avec streaming et insertion par batch pour optimiser la mémoire
"""
import time
from datetime import datetime
from string import Template
from typing import Dict, List, Generator
from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.logger import get_logger

logger = get_logger(__name__)


class AMUEDataImporter:
    """Gère l'import des données depuis l'API vers PostgreSQL avec streaming"""

    # Taille de batch par défaut pour l'insertion
    DEFAULT_BATCH_SIZE = 5000

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        self.api_hook = api_hook
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        self._conn = None  # Cache de connexion

        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'universite' doit être définie")
        try:
            endpointtbl = VarMgr.get('api_endpoint_table')
        except KeyError:
            raise AirflowException("La variable 'api_endpoint_table' doit être définie")
        try:
            self.endpoint = Template(endpointtbl).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        self.max_retries = int(VarMgr.get('amue_api_max_retries', default='3'))
        self.retry_delay = int(VarMgr.get('amue_api_retry_delay_seconds', default='30'))
        self.batch_size = int(VarMgr.get('amue_import_batch_size', default=str(self.DEFAULT_BATCH_SIZE)))

    def _get_connection(self):
        """Retourne une connexion réutilisable"""
        if self._conn is None or self._conn.closed:
            self._conn = self.postgres_hook.get_conn()
        return self._conn

    def _close_connection(self):
        """Ferme proprement la connexion"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def import_table(self, table_name: str, columns: List[str], primary_keys: List[str],
                     import_config: Dict) -> Dict:
        """Importe les données d'une table avec streaming"""
        logger.info(f"Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        try:
            # Détermine si on utilise UPSERT (vérifie avant le stream)
            import_type = import_config.get('import_type', 'full')
            use_upsert = import_type == 'differential' and bool(primary_keys)

            # Stream les données et insère par batch
            rows_inserted, rows_fetched = self._stream_and_insert(
                table_name,
                columns,
                primary_keys,
                import_config,
                use_upsert
            )

            return {
                'table_name': table_name,
                'rows_inserted': rows_inserted,
                'rows_fetched': rows_fetched,
                'import_type': import_type,
                'finger_print': import_config.get('finger_print', ''),
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Erreur import {table_name}: {e}")
            raise
        finally:
            self._close_connection()

    def _stream_and_insert(self, table_name: str, columns: List[str],
                           primary_keys: List[str], import_config: Dict,
                           use_upsert: bool) -> tuple:
        """
        Stream les données depuis l'API et insère par batch

        Returns:
            Tuple (rows_inserted, rows_fetched)
        """
        # Construit la requête SQL
        insert_sql = self._build_insert_sql(table_name, columns, primary_keys, use_upsert)

        conn = self._get_connection()
        cursor = conn.cursor()

        total_inserted = 0
        total_fetched = 0
        batch = []

        try:
            # Stream les données depuis l'API
            for row in self._fetch_data_stream(table_name, import_config):
                total_fetched += 1

                # Prépare le record
                row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}
                record = tuple(row_lower.get(col, None) for col in columns)
                batch.append(record)

                # Insert quand batch plein
                if len(batch) >= self.batch_size:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()

                    total_inserted += len(batch)
                    logger.info(f"{table_name}: {total_inserted:,} lignes insérées")

                    batch.clear()  # Libère mémoire

            # Insert reste du batch
            if batch:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)

            logger.info(f"{table_name}: Total {total_inserted:,}/{total_fetched:,} lignes")
            return total_inserted, total_fetched

        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur insertion {table_name} après {total_inserted} lignes: {e}")
            raise AirflowException(f"Import error: {e}")
        finally:
            cursor.close()

    def _fetch_data_stream(self, table_name: str, import_config: Dict) -> Generator[Dict, None, None]:
        """
        Récupère les données en streaming (générateur)

        Yields:
            Ligne de données une par une
        """
        base_params = self._build_query_params(table_name, import_config)
        skip = 0
        page = 1

        while True:
            params = base_params.copy()
            params['skip'] = skip

            rows, has_more = self._fetch_page(params, page)

            if not rows:
                break

            for row in rows:
                yield row  # Yield au lieu d'accumuler

            if not has_more:
                break

            skip += len(rows)
            page += 1

    def _build_query_params(self, table_name: str, import_config: Dict) -> Dict:
        """Construit les paramètres de requête"""
        params = {
            'nom': table_name.upper(),
            'f': 'json'
        }

        # Import différentiel
        import_type = import_config.get('import_type', 'full')
        delta_column = import_config.get('delta', '')
        last_import = import_config.get('last_import', '')

        if import_type == 'differential' and delta_column and last_import:
            last_import_str = self._format_date_for_query(last_import)
            params['q'] = f"{delta_column}='{last_import_str}'"
            logger.info(f"Delta: {params['q']}")

        return params

    def _format_date_for_query(self, date_str: str) -> str:
        """Formate une date pour la requête"""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y%m%d')
        except Exception:
            return date_str.replace('-', '')[:8]

    def _fetch_page(self, params: Dict, page: int) -> tuple:
        """Récupère une page de données avec retry"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Page {page} (skip={params['skip']}, attempt {attempt + 1}/{self.max_retries})")

                response = self.api_hook.call_api(self.endpoint, params)

                if not isinstance(response, dict) or 'data' not in response:
                    raise ValueError("Format réponse invalide")

                data_obj = response['data']
                rows = data_obj.get('row', [])

                if not isinstance(rows, list):
                    rows = [rows] if rows else []

                if rows:
                    logger.info(f"{len(rows)} lignes récupérées")

                # Vérifie s'il y a plus de données
                count = data_obj.get('count', 0)
                top = data_obj.get('top', 99)
                has_more = len(rows) >= top and (params['skip'] + len(rows)) < count

                return rows, has_more

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")

                if attempt < self.max_retries - 1:
                    logger.info(f"Retry dans {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    error_msg = f"Impossible récupérer données après {self.max_retries} tentatives"
                    logger.error(error_msg)
                    raise AirflowException(error_msg)

        return [], False

    def _build_insert_sql(self, table_name: str, columns: List[str],
                          primary_keys: List[str], use_upsert: bool) -> str:
        """Construit la requête SQL d'insertion avec identifiants sécurisés"""
        # Identifiants sécurisés (protection SQL injection)
        table_id = sql.Identifier(table_name)
        column_ids = [sql.Identifier(col) for col in columns]

        placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        column_list = sql.SQL(', ').join(column_ids)

        if use_upsert and primary_keys:
            pk_ids = [sql.Identifier(pk) for pk in primary_keys]
            update_cols = [sql.Identifier(col) for col in columns if col not in primary_keys]

            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({pks})
                DO UPDATE SET {updates}
            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders,
                pks=sql.SQL(', ').join(pk_ids),
                updates=sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(col, col)
                    for col in update_cols
                ])
            )
        else:
            query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
            """).format(
                table=table_id,
                columns=column_list,
                placeholders=placeholders
            )

        return query.as_string(self._get_connection())
