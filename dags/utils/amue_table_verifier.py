"""
Vérificateur de structure et statut des tables AMUE
"""
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from . import parse_column_definition, compute_structure_hash


class AMUETableVerifier:
    """Vérifie le statut et la structure des tables"""

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        self.api_hook = api_hook
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        self.environment = Variable.get('environment', default='production')
        try:
            univ = Variable.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUETableVerifier")
        try:
            endpointadm = Variable.get('api_endpoint_admin')
        except KeyError:
            raise AirflowException("La variable 'api_endpoint_admin' doit être définie pour initialiser AMUETableVerifier")
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

    def verify_status(self, table_info: Dict) -> Dict:
        """Vérifie le statut d'une table"""
        table_name = table_info.get('name', 'unknown')
        print(f"[STATUS_CHECK] Vérification statut: {table_name}")

        current_status = table_info.get('current_status', {})
        status = current_status.get('status', 'UNKNOWN')

        if status != 'OK':
            error_msg = f"Table {table_name} status={status} (attendu: OK)"
            print(f"[ERROR] {error_msg}")
            return {
                'table_name': table_name,
                'status': 'error',
                'status_ok': False,
                'error': error_msg,
                'details': current_status
            }

        print(f"[STATUS_CHECK] {table_name}: OK")
        return {
            'table_name': table_name,
            'status': 'success',
            'status_ok': True,
            'error': None,
            'details': current_status
        }

    def verify_structure(self, table_info: Dict) -> Dict:
        """Vérifie la structure d'une table"""
        table_name = table_info.get('name', 'unknown')
        print(f"[STRUCTURE_CHECK] Vérification structure: {table_name}")

        try:
            # Récupère la structure depuis l'API
            columns = self._fetch_structure(table_name)
            finger_print = compute_structure_hash(columns)

            # Récupère les clés primaires
            primary_keys = table_info.get('primary_key', '')
            if not primary_keys:
                primary_keys = self._fetch_primary_keys(table_name)

            # Vérifie l'existence de la table
            exists = self._table_exists(table_name)

            # Vérifie les changements de structure
            structure_changed = self._check_structure_change(
                table_name,
                finger_print,
                table_info.get('finger_print', ''),
                exists
            )

            if structure_changed and self.environment == 'production':
                error_msg = f"Changement structure détecté en production"
                print(f"[ERROR] {error_msg}")
                return self._error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, True)

            if not exists and self.environment == 'production':
                error_msg = f"Table {table_name} n'existe pas en production"
                print(f"[ERROR] {error_msg}")
                return self._error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, False)

            print(f"[STRUCTURE_CHECK] {table_name}: OK")
            return {
                'table_name': table_name,
                'status': 'success',
                'structure_ok': True,
                'error': None,
                'columns': columns,
                'finger_print': finger_print,
                'primary_keys': primary_keys,
                'exists': exists,
                'structure_changed': structure_changed
            }

        except Exception as e:
            error_msg = f"Erreur vérification structure {table_name}: {e}"
            print(f"[ERROR] {error_msg}")
            return self._error_result(table_name, error_msg, [], '', '', False, False)

    def _fetch_structure(self, table_name: str) -> List[Dict]:
        """Récupère la structure depuis l'API"""
        params = {'get': f'{table_name}.def', 'f': 'json'}
        structure_response = self.api_hook.call_api(self.endpoint, params)

        if isinstance(structure_response, str):
            columns_def = structure_response.strip()
        elif isinstance(structure_response, dict):
            columns_def = structure_response.get('definition') or str(structure_response)
        else:
            columns_def = str(structure_response)

        columns = []
        for col_def in columns_def.split(','):
            col_def = col_def.strip()
            if not col_def:
                continue

            parts = col_def.split(None, 1)
            if len(parts) >= 2:
                col_name = parts[0].strip()
                col_type = parts[1].strip()
                pg_type = parse_column_definition(col_type)

                columns.append({
                    'name': col_name,
                    'type_original': col_type,
                    'type_postgres': pg_type
                })

        if not columns:
            raise ValueError("Aucune colonne trouvée")

        return columns

    def _fetch_primary_keys(self, table_name: str) -> str:
        """Récupère les clés primaires depuis l'API"""
        print(f"[STRUCTURE_CHECK] Récupération des clés...")
        params = {'get': f'{table_name}.keys', 'f': 'json'}
        keys_response = self.api_hook.call_api(self.endpoint, params)

        if isinstance(keys_response, str):
            return keys_response.strip()
        elif isinstance(keys_response, list):
            return ','.join(keys_response)
        elif isinstance(keys_response, dict):
            return ','.join(keys_response.get('keys', []))

        return ''

    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe en base"""
        check_sql = """
                    SELECT EXISTS (SELECT \
                                   FROM information_schema.tables \
                                   WHERE table_schema = 'splus' \
                                     AND table_name = %s) \
                    """
        result = self.postgres_hook.get_first(check_sql, parameters=(table_name.lower(),))
        return result[0] if result else False

    def _check_structure_change(self, table_name: str, new_fingerprint: str,
                                old_fingerprint: str, exists: bool) -> bool:
        """Vérifie si la structure a changé"""
        if not exists or not old_fingerprint or not new_fingerprint:
            return False

        changed = (old_fingerprint != new_fingerprint)
        if changed:
            print(f"[STRUCTURE_CHECK] Changement: {old_fingerprint} -> {new_fingerprint}")

        return changed

    def _error_result(self, table_name: str, error: str, columns: List,
                      finger_print: str, primary_keys: str, exists: bool,
                      structure_changed: bool) -> Dict:
        """Construit un résultat d'erreur"""
        return {
            'table_name': table_name,
            'status': 'error',
            'structure_ok': False,
            'error': error,
            'columns': columns,
            'finger_print': finger_print,
            'primary_keys': primary_keys,
            'exists': exists,
            'structure_changed': structure_changed
        }