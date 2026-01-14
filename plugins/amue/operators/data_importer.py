"""
Gestionnaire d'import des données depuis l'API AMUE
"""
import time
from datetime import datetime
from string import Template
from typing import Dict, List
from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr


class AMUEDataImporter:
    """Gère l'import des données depuis l'API vers PostgreSQL"""

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        self.api_hook = api_hook
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUEDataImporter")
        try:
            endpointtbl = VarMgr.get('api_endpoint_table')
        except KeyError:
            raise AirflowException(
                "La variable 'api_endpoint_table' doit être définie pour initialiser AMUEDataImporter")
        try:
            self.endpoint = Template(endpointtbl).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        self.max_retries = int(VarMgr.get('amue_api_max_retries', default='3'))
        self.retry_delay = int(VarMgr.get('amue_api_retry_delay_seconds', default='30'))

    def import_table(self, table_name: str, columns: List[str], primary_keys: List[str],
                     import_config: Dict) -> Dict:
        """Importe les données d'une table"""
        print(f"[IMPORT] Table: {table_name}, type: {import_config.get('import_type', 'full')}")

        # Récupère les données depuis l'API
        all_data = self._fetch_data(table_name, import_config)

        if not all_data:
            return {
                'table_name': table_name,
                'rows_inserted': 0,
                'rows_fetched': 0,
                'import_type': import_config.get('import_type', 'full'),
                'finger_print': import_config.get('finger_print', ''),
                'status': 'success'
            }

        # Insère les données
        rows_inserted = self._insert_data(
            table_name,
            columns,
            primary_keys,
            all_data,
            import_config
        )

        return {
            'table_name': table_name,
            'rows_inserted': rows_inserted,
            'rows_fetched': len(all_data),
            'import_type': import_config.get('import_type', 'full'),
            'finger_print': import_config.get('finger_print', ''),
            'status': 'success'
        }

    def _fetch_data(self, table_name: str, import_config: Dict) -> List[Dict]:
        """Récupère toutes les données depuis l'API avec pagination"""
        base_params = self._build_query_params(table_name, import_config)

        all_data = []
        skip = 0
        page = 1
        has_more = True

        while has_more:
            params = base_params.copy()
            params['skip'] = skip

            rows, has_more = self._fetch_page(params, page)

            if rows:
                all_data.extend(rows)
                skip += len(rows)
                page += 1

        print(f"[IMPORT] Total: {len(all_data)} lignes")
        return all_data

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
            print(f"[IMPORT] Delta: {params['q']}")

        return params

    def _format_date_for_query(self, date_str: str) -> str:
        """Formate une date pour la requête"""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y%m%d')
        except:
            return date_str.replace('-', '')[:8]

    def _fetch_page(self, params: Dict, page: int) -> tuple:
        """Récupère une page de données avec retry"""
        for attempt in range(self.max_retries):
            try:
                print(f"[IMPORT] Page {page} (skip={params['skip']}, "
                      f"attempt {attempt + 1}/{self.max_retries})")

                response = self.api_hook.call_api(self.endpoint, params)

                if not isinstance(response, dict) or 'data' not in response:
                    raise ValueError("Format réponse invalide")

                data_obj = response['data']
                rows = data_obj.get('row', [])

                if not isinstance(rows, list):
                    rows = [rows] if rows else []

                if rows:
                    print(f"[IMPORT] {len(rows)} lignes récupérées")

                # Vérifie s'il y a plus de données
                count = data_obj.get('count', 0)
                top = data_obj.get('top', 99)
                has_more = len(rows) >= top and (params['skip'] + len(rows)) < count

                return rows, has_more

            except Exception as e:
                print(f"[ERROR] Attempt {attempt + 1} failed: {e}")

                if attempt < self.max_retries - 1:
                    print(f"[RETRY] Wait {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    error_msg = f"Impossible récupérer données après {self.max_retries} tentatives"
                    print(f"[ERROR] {error_msg}")
                    raise AirflowException(error_msg)

        return [], False

    def _insert_data(self, table_name: str, columns: List[str],
                     primary_keys: List[str], data: List[Dict],
                     import_config: Dict) -> int:
        """Insère les données dans PostgreSQL"""
        import_type = import_config.get('import_type', 'full')
        use_upsert = self._should_use_upsert(table_name, primary_keys, data, import_type)

        # Construit la requête SQL
        insert_sql = self._build_insert_sql(table_name, columns, primary_keys, use_upsert)

        # Prépare les records
        records = self._prepare_records(data, columns)

        # Exécute l'insertion
        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.executemany(insert_sql, records)
            conn.commit()
            rows_inserted = cursor.rowcount

            print(f"[IMPORT] {rows_inserted} lignes insérées")
            return rows_inserted

        except Exception as e:
            conn.rollback()
            error_msg = f"Erreur insertion {table_name}: {e}"
            print(f"[ERROR] {error_msg}")
            raise AirflowException(error_msg)
        finally:
            cursor.close()
            conn.close()

    def _should_use_upsert(self, table_name: str, primary_keys: List[str],
                           data: List[Dict], import_type: str) -> bool:
        """Détermine si on doit utiliser UPSERT"""
        if import_type != 'differential' or not primary_keys or not data:
            return False

        # Vérifie si des données existent déjà
        sample_row = data[0]
        row_lower = {k.lower(): v for k, v in sample_row.items()} if isinstance(sample_row, dict) else {}

        pk_values = [row_lower.get(pk, None) for pk in primary_keys]
        if not all(v is not None for v in pk_values):
            return False

        where_clause = ' AND '.join([f"{pk} = %s" for pk in primary_keys])
        check_sql = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {where_clause})"

        result = self.postgres_hook.get_first(check_sql, parameters=tuple(pk_values))
        return result[0] if result else False

    def _build_insert_sql(self, table_name: str, columns: List[str],
                          primary_keys: List[str], use_upsert: bool) -> str:
        # Identifiants sécurisés
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

        return query.as_string(self.postgres_hook.get_conn())

    def _prepare_records(self, data: List[Dict], columns: List[str]) -> List[tuple]:
        """Prépare les records pour l'insertion"""
        records = []
        for row in data:
            row_lower = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {}
            record = tuple(row_lower.get(col, None) for col in columns)
            records.append(record)
        return records