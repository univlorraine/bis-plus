"""
Vérificateur de structure et statut des tables AMUE.

Orchestre :
    1. Le STATUT de la table côté API (doit être "OK")
    2. La STRUCTURE de la table (colonnes, types) → APIStructureFetcher
    3. Le FINGERPRINT pour détecter les changements de structure → fingerprint_comparator

Si les fingerprints n'existent pas encore (premier import), ils sont
calculés et persistés. Si la structure a changé, l'import est BLOQUÉ
jusqu'à validation manuelle.
"""
import json
import logging
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from amue.exceptions import AMUESchemaError
from amue.operators.table_management.fingerprint_comparator import (
    check_fingerprint_changes,
    compute_structure_diff,
    format_pg_type,
)
from amue.operators.table_management.structure_fetcher import (
    APIStructureFetcher,
    split_column_defs,
)
from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from common.utils.database.hooks import resolve_postgres_hook
from common.utils.fingerprint import compute_structure_hash_with_pk

logger = logging.getLogger(__name__)


# Re-exports pour rétro-compat des tests/imports existants.
_check_fingerprint_changes = check_fingerprint_changes


def _error_result(table_name: str, error: str, columns: List,
                  fingerprint_API: str, fingerprint_local: str,
                  primary_keys: str, exists: bool,
                  structure_changed: bool) -> Dict:
    return {
        'table_name': table_name,
        'status': 'error',
        'structure_ok': False,
        'error': error,
        'columns': columns,
        'fingerprint_API': fingerprint_API,
        'fingerprint_local': fingerprint_local,
        'primary_keys': primary_keys,
        'exists': exists,
        'structure_changed': structure_changed,
    }


class AMUETableVerifier:
    """Vérifie le statut et la structure des tables AMUE."""

    def __init__(self, api_hook, postgres_hook: PostgresHook = None, target_schema: str = None):
        if api_hook is None:
            raise ValueError("api_hook est requis pour AMUETableVerifier")

        self.api_hook = api_hook
        self.target_schema = target_schema
        self.postgres_hook = resolve_postgres_hook(postgres_hook, target_schema)

        univ = VarMgr.get_required(
            'universite',
            "La variable 'univ' doit être définie pour initialiser AMUETableVerifier",
        )
        endpointadm = VarMgr.get_required(
            'api_endpoint_admin',
            "La variable 'api_endpoint_admin' doit être définie pour initialiser AMUETableVerifier",
        )
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        self._fetcher = APIStructureFetcher(api_hook, self.endpoint)

        if target_schema:
            logger.info(f"[VERIFY] Schéma cible blue/green: {target_schema}")

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    def verify_status(self, table_info: Dict) -> Dict:
        """Vérifie le statut d'une table côté API."""
        table_name = table_info.get('table_name', 'unknown')
        logger.info(f"[STATUS_CHECK] Vérification statut: {table_name}")

        current_status = table_info.get('current_status', {})
        status = current_status.get('status', 'UNKNOWN')

        if status != 'OK':
            status_details = json.dumps(current_status, ensure_ascii=False, default=str)
            error_msg = f"Table {table_name} status={status} (attendu: OK). Details: {status_details}"
            logger.error(f"[ERROR] {error_msg}")
            return {
                'table_name': table_name,
                'status': 'error',
                'status_ok': False,
                'error': error_msg,
                'details': current_status,
            }

        logger.info(f"[STATUS_CHECK] {table_name}: OK")
        return {
            'table_name': table_name,
            'status': 'success',
            'status_ok': True,
            'error': None,
            'details': current_status,
        }

    def verify_structure(self, table_info: Dict) -> Dict:
        """
        Vérifie la structure d'une table.

        Calcule deux fingerprints :
            - fingerprint_API : structure originale + PKs API (toujours fetchées)
            - fingerprint_local : structure transformée PG + PKs config (fallback API)
        """
        table_name = table_info.get('table_name', 'unknown')
        logger.info(f"[STRUCTURE_CHECK] Vérification structure: {table_name}")
        logger.info(f"[STRUCTURE_CHECK] table_info keys: {list(table_info.keys())}")
        logger.info(f"[STRUCTURE_CHECK] table_info['primary_key'] = '{table_info.get('primary_key', 'NOT_SET')}'")

        try:
            columns = self._fetch_structure(table_name)
            api_pks = self._fetch_primary_keys(table_name)
            logger.info(f"[STRUCTURE_CHECK] PKs API: '{api_pks}'")

            config_pks = table_info.get('primary_key', '')
            logger.info(f"[STRUCTURE_CHECK] PKs config: '{config_pks}'")

            if not config_pks and api_pks:
                logger.info(f"[STRUCTURE_CHECK] => Appel _save_primary_keys({table_name}, {api_pks})")
                self._save_primary_keys(table_name, api_pks)

            if not api_pks and not config_pks:
                logger.warning(f"[WARN] Aucune clé primaire trouvée pour {table_name}")

            fingerprint_API = compute_structure_hash_with_pk(columns, api_pks, type_key='type_original')
            fingerprint_local = compute_structure_hash_with_pk(
                columns, config_pks or api_pks, type_key='type_postgres'
            )
            primary_keys = config_pks or api_pks

            exists = self._table_exists(table_name)

            fp_changes = check_fingerprint_changes(
                table_name,
                fingerprint_API, table_info.get('fingerprint_API', ''),
                fingerprint_local, table_info.get('fingerprint_local', ''),
                exists,
            )
            structure_changed = fp_changes['api_changed'] or fp_changes['ul_changed']

            if not exists:
                logger.info(f"[STRUCTURE_CHECK] {table_name}: table absente, sera creee automatiquement")

            logger.info(f"[STRUCTURE_CHECK] {table_name}: OK")
            return {
                'table_name': table_name,
                'status': 'success',
                'structure_ok': True,
                'error': None,
                'columns': columns,
                'fingerprint_API': fingerprint_API,
                'fingerprint_local': fingerprint_local,
                'primary_keys': primary_keys,
                'exists': exists,
                'structure_changed': structure_changed,
            }
        except Exception as e:
            error_msg = f"Erreur vérification structure {table_name} [{type(e).__name__}]: {e}"
            logger.error(f"[ERROR] {error_msg}")
            return _error_result(table_name, error_msg, [], '', '', '', False, False)

    def verify_table(self, table_info: Dict) -> Dict:
        """Vérifie une table : statut + structure + fingerprint."""
        table_name = table_info.get('table_name', 'unknown')
        logger.info(f"[VERIFY] === Vérification complète: {table_name} ===")

        status_result = self.verify_status(table_info)
        if status_result.get('status') == 'error':
            return {
                'table_name': table_name,
                'status': 'error',
                'phase': 'status',
                'error': status_result.get('error'),
                'original_info': table_info,
            }

        structure_result = self.verify_structure(table_info)
        if structure_result.get('status') == 'error':
            return {
                'table_name': table_name,
                'status': 'error',
                'phase': 'structure',
                'error': structure_result.get('error'),
                'original_info': table_info,
            }

        old_fp_api = table_info.get('fingerprint_API', '')
        new_fp_api = structure_result.get('fingerprint_API', '')
        old_fp_local = table_info.get('fingerprint_local', '')
        new_fp_local = structure_result.get('fingerprint_local', '')

        changes = []
        if old_fp_api and new_fp_api and old_fp_api != new_fp_api:
            changes.append(
                f"fingerprint_API: {old_fp_api[:16]}... -> {new_fp_api[:16]}...\n"
                f"  Cause: L'AMUE a modifie la structure source de la table."
            )
        if old_fp_local and new_fp_local and old_fp_local != new_fp_local:
            try:
                existing = self._fetch_existing_columns(table_name)
                diff_detail = compute_structure_diff(existing, structure_result.get('columns', []))
            except Exception as e:
                diff_detail = f"(impossible de calculer le diff: {e})"
            changes.append(
                f"fingerprint_local: {old_fp_local[:16]}... -> {new_fp_local[:16]}...\n"
                f"  {diff_detail}"
            )

        if changes:
            error_msg = (
                f"CHANGEMENT DE STRUCTURE DÉTECTÉ pour {table_name}\n"
                + "\n".join(changes)
                + "\nAction requise: Vérifier les changements et mettre à jour manuellement."
            )
            logger.error(f"[ERROR] {error_msg}")
            return {
                'table_name': table_name,
                'status': 'error',
                'phase': 'fingerprint',
                'error': error_msg,
                'original_info': table_info,
            }

        logger.info(f"[VERIFY] {table_name}: Toutes les vérifications OK")

        updated_info = dict(table_info)
        if not old_fp_api and new_fp_api:
            updated_info['fingerprint_API'] = new_fp_api
            logger.info(f"[VERIFY] fingerprint_API initialisé: {new_fp_api[:16]}...")
        if not old_fp_local and new_fp_local:
            updated_info['fingerprint_local'] = new_fp_local
            logger.info(f"[VERIFY] fingerprint_local initialisé: {new_fp_local[:16]}...")

        return {
            'table_name': table_name,
            'status': 'success',
            'phase': 'complete',
            'error': None,
            'columns': structure_result.get('columns', []),
            'primary_keys': structure_result.get('primary_keys', ''),
            'fingerprint_API': new_fp_api,
            'fingerprint_local': new_fp_local,
            'exists': structure_result.get('exists', False),
            'original_info': updated_info,
        }

    # =========================================================================
    # WRAPPERS DÉLÉGUÉS — préservent l'API testée par les tests existants
    # =========================================================================

    def _fetch_structure(self, table_name: str) -> List[Dict]:
        return self._fetcher.fetch_structure(table_name)

    def _fetch_primary_keys(self, table_name: str) -> str:
        return self._fetcher.fetch_primary_keys(table_name)

    @staticmethod
    def _split_column_defs(columns_def: str) -> list:
        return split_column_defs(columns_def)

    @staticmethod
    def _format_pg_type(data_type: str, char_len, num_prec, num_scale) -> str:
        return format_pg_type(data_type, char_len, num_prec, num_scale)

    def _save_primary_keys(self, table_name: str, primary_keys: str) -> None:
        """Persiste les PKs dans splus_admin.amue_tables."""
        logger.info(f"[SAVE_PK] Tentative sauvegarde PKs pour {table_name}: '{primary_keys}'")
        if not primary_keys:
            logger.warning(f"[SAVE_PK] PKs vides pour {table_name}, abandon")
            return
        from amue.services.table_config_manager import TableConfigManager
        TableConfigManager().save_primary_keys(table_name, primary_keys)
        logger.info(f"[SAVE_PK] SUCCESS - PKs sauvegardées pour {table_name}")

    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe en base dans le schéma cible."""
        schema_to_check = self.target_schema if self.target_schema else 'splus'
        check_sql = """
                    SELECT EXISTS (SELECT 1
                                   FROM information_schema.tables
                                   WHERE table_schema = %s
                                     AND table_name = %s)
                    """
        result = self.postgres_hook.get_first(check_sql, parameters=(schema_to_check, table_name.lower()))
        return result[0] if result else False

    def _fetch_existing_columns(self, table_name: str) -> List[Dict]:
        """Récupère les colonnes existantes en base via information_schema."""
        schema_to_check = self.target_schema if self.target_schema else 'splus'
        sql = """
            SELECT column_name, data_type,
                   character_maximum_length,
                   numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name NOT IN ('_source', '_imported_at')
            ORDER BY ordinal_position
        """
        rows = self.postgres_hook.get_records(sql, parameters=(schema_to_check, table_name.lower()))
        return [
            {
                'name': row[0].upper(),
                'type_postgres': format_pg_type(row[1], row[2], row[3], row[4]),
            }
            for row in rows
        ]

    def _compute_structure_diff(self, table_name: str, new_columns: List[Dict]) -> str:
        """Wrapper conservé pour rétro-compat. Délègue à `compute_structure_diff`."""
        try:
            existing = self._fetch_existing_columns(table_name)
        except Exception as e:
            return f"(impossible de calculer le diff: {e})"
        return compute_structure_diff(existing, new_columns)
