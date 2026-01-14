"""
Vérificateur de structure et statut des tables AMUE
Mise à jour avec fingerprint incluant les clés primaires
"""
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.transformers import compute_structure_hash_with_pk, parse_column_definition


def _error_result(table_name: str, error: str, columns: List,
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


def _check_structure_change(table_name: str, new_fingerprint: str,
                            old_fingerprint: str, exists: bool) -> bool:
    """Vérifie si la structure a changé"""
    if not exists or not old_fingerprint or not new_fingerprint:
        return False

    changed = (old_fingerprint != new_fingerprint)
    if changed:
        print(f"[STRUCTURE_CHECK] {table_name}: Changement détecté")
        print(f"  Ancien fingerprint: {old_fingerprint[:16]}...")
        print(f"  Nouveau fingerprint: {new_fingerprint[:16]}...")

    return changed


class AMUETableVerifier:
    """Vérifie le statut et la structure des tables"""

    def __init__(self, api_hook, postgres_hook: PostgresHook = None):
        self.api_hook = api_hook
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        self.environment = VarMgr.get('environment', default='production')
        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUETableVerifier")
        try:
            endpointadm = VarMgr.get('api_endpoint_admin')
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
        """
        Vérifie la structure d'une table

        NOUVEAU:
        - Récupère automatiquement les clés primaires si absentes
        - Calcule le fingerprint avec les clés primaires
        """
        table_name = table_info.get('name', 'unknown')
        print(f"[STRUCTURE_CHECK] Vérification structure: {table_name}")

        try:
            # Récupère la structure depuis l'API
            columns = self._fetch_structure(table_name)

            # Récupère les clés primaires
            primary_keys = table_info.get('primary_key', '')
            needs_pk_update = table_info.get('needs_pk_update', False)

            if not primary_keys or needs_pk_update:
                print(f"[STRUCTURE_CHECK] Clés primaires absentes ou à mettre à jour")
                print(f"[STRUCTURE_CHECK] Récupération depuis API...")
                primary_keys = self._fetch_primary_keys(table_name)

                if primary_keys:
                    print(f"[STRUCTURE_CHECK] ✓ Clés primaires récupérées: {primary_keys}")
                else:
                    print(f"[WARN] Aucune clé primaire trouvée pour {table_name}")
            else:
                print(f"[STRUCTURE_CHECK] Clés primaires existantes: {primary_keys}")

            # NOUVEAU: Calcul du fingerprint avec les clés primaires
            finger_print = compute_structure_hash_with_pk(columns, primary_keys)

            # Vérifie l'existence de la table
            exists = self._table_exists(table_name)

            # Vérifie les changements de structure
            structure_changed = _check_structure_change(
                table_name,
                finger_print,
                table_info.get('finger_print', ''),
                exists
            )

            if structure_changed and self.environment == 'production':
                error_msg = f"Changement structure détecté en production"
                print(f"[ERROR] {error_msg}")
                return _error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, True)

            if not exists and self.environment == 'production':
                error_msg = f"Table {table_name} n'existe pas en production"
                print(f"[ERROR] {error_msg}")
                return _error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, False)

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
                'structure_changed': structure_changed,
                'needs_pk_update': needs_pk_update  # Pour mise à jour ultérieure
            }

        except Exception as e:
            error_msg = f"Erreur vérification structure {table_name}: {e}"
            print(f"[ERROR] {error_msg}")
            return _error_result(table_name, error_msg, [], '', '', False, False)

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
        """
        Récupère les clés primaires depuis l'API

        NOUVEAU: Logs plus détaillés pour traçabilité
        """
        print(f"[STRUCTURE_CHECK] Appel API pour clés primaires de {table_name}")

        params = {'get': f'{table_name}.keys', 'f': 'json'}

        try:
            keys_response = self.api_hook.call_api(self.endpoint, params)

            # Gestion des différents formats de réponse
            if isinstance(keys_response, str):
                result = keys_response.strip()
            elif isinstance(keys_response, list):
                result = ','.join(str(k) for k in keys_response if k)
            elif isinstance(keys_response, dict):
                result = ','.join(str(k) for k in keys_response.get('keys', []) if k)
            else:
                print(f"[WARN] Format de réponse inattendu pour les clés: {type(keys_response)}")
                result = ''

            if result:
                print(f"[STRUCTURE_CHECK] Clés trouvées: {result}")
            else:
                print(f"[WARN] Aucune clé primaire retournée par l'API")

            return result

        except Exception as e:
            print(f"[ERROR] Erreur lors de la récupération des clés: {str(e)}")
            return ''

    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe en base"""
        check_sql = """
                    SELECT EXISTS (SELECT 1
                                   FROM information_schema.tables 
                                   WHERE table_schema = 'splus' 
                                     AND table_name = %s)
                    """
        result = self.postgres_hook.get_first(check_sql, parameters=(table_name.lower(),))
        return result[0] if result else False