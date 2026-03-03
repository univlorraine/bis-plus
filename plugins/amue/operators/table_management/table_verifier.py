"""
Vérificateur de structure et statut des tables AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module vérifie que les tables sont prêtes pour l'import en validant :
    1. Le STATUT de la table côté API (doit être "OK")
    2. La STRUCTURE de la table (colonnes, types)
    3. Le FINGERPRINT pour détecter les changements de structure

PROCESSUS DE VÉRIFICATION :

    ┌─────────────────────────────────────────────────────────────────┐
    │                     verify_table()                              │
    │                          │                                      │
    │           ┌──────────────┼──────────────┐                       │
    │           ▼              ▼              ▼                       │
    │    verify_status()  verify_structure()  verify_fingerprint()    │
    │           │              │              │                       │
    │           ▼              ▼              ▼                       │
    │      Statut OK ?    Colonnes OK ?   Structure changée ?         │
    │           │              │              │                       │
    │           └──────────────┴──────────────┘                       │
    │                          │                                      │
    │                          ▼                                      │
    │              Résultat de vérification                           │
    └─────────────────────────────────────────────────────────────────┘

================================================================================
FINGERPRINT (EMPREINTE DE STRUCTURE)
================================================================================

Le fingerprint est un hash SHA256 calculé à partir de :
    - Noms des colonnes (ordonnés)
    - Types PostgreSQL des colonnes
    - Clés primaires (si définies)

Utilité :
    - Détecte les changements de structure entre deux exécutions
    - En production : BLOQUE l'import si structure modifiée
    - En dev : ALERTE mais continue l'import

Exemple de changement détecté :
    - Ajout/suppression de colonne
    - Modification de type
    - Changement de clé primaire

================================================================================
RÉCUPÉRATION AUTOMATIQUE DES CLÉS PRIMAIRES
================================================================================

Si les clés primaires ne sont pas définies dans la configuration,
le vérificateur les récupère automatiquement depuis l'API AMUE.
Cela permet de :
    - Simplifier la configuration initiale
    - S'adapter aux changements de clés côté AMUE
    - Garantir la cohérence des UPSERT

================================================================================
CONFIGURATION
================================================================================

Variables Airflow :
    - universite : Code université pour l'endpoint API
    - api_endpoint_admin : Template d'URL admin avec $univ

================================================================================
"""
import logging
import sys
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from amue.exceptions import AMUESchemaError
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.database.hooks import create_postgres_hook
from amue.utils.transformers import compute_structure_hash_with_pk, parse_column_definition

logger = logging.getLogger(__name__)

_META_COLUMNS = {'_SOURCE', '_IMPORTED_AT'}


def _error_result(table_name: str, error: str, columns: List,
                  fingerprint_API: str, fingerprint_UL: str,
                  primary_keys: str, exists: bool,
                  structure_changed: bool) -> Dict:
    """Construit un résultat d'erreur"""
    return {
        'table_name': table_name,
        'status': 'error',
        'structure_ok': False,
        'error': error,
        'columns': columns,
        'fingerprint_API': fingerprint_API,
        'fingerprint_UL': fingerprint_UL,
        'primary_keys': primary_keys,
        'exists': exists,
        'structure_changed': structure_changed
    }


def _check_fingerprint_changes(table_name: str, new_fp_api: str, old_fp_api: str,
                               new_fp_ul: str, old_fp_ul: str, exists: bool) -> Dict:
    """Vérifie quels fingerprints ont changé."""
    if not exists:
        return {'api_changed': False, 'ul_changed': False}

    api_changed = bool(old_fp_api and new_fp_api and old_fp_api != new_fp_api)
    ul_changed = bool(old_fp_ul and new_fp_ul and old_fp_ul != new_fp_ul)

    if api_changed:
        logger.info(f"[STRUCTURE_CHECK] {table_name}: fingerprint_API changé")
        logger.info(f"  Ancien: {old_fp_api[:16]}...")
        logger.info(f"  Nouveau: {new_fp_api[:16]}...")
    if ul_changed:
        logger.info(f"[STRUCTURE_CHECK] {table_name}: fingerprint_UL changé")
        logger.info(f"  Ancien: {old_fp_ul[:16]}...")
        logger.info(f"  Nouveau: {new_fp_ul[:16]}...")

    return {'api_changed': api_changed, 'ul_changed': ul_changed}


class AMUETableVerifier:
    """Vérifie le statut et la structure des tables"""

    def __init__(self, api_hook, postgres_hook: PostgresHook = None, target_schema: str = None):
        # Validation des paramètres requis
        if api_hook is None:
            raise ValueError("api_hook est requis pour AMUETableVerifier")

        self.api_hook = api_hook
        self.target_schema = target_schema

        # Crée le hook avec le schéma cible si spécifié
        if postgres_hook:
            self.postgres_hook = postgres_hook
        elif target_schema:
            self.postgres_hook = create_postgres_hook(bluegreen_schema=target_schema)
        else:
            self.postgres_hook = create_postgres_hook()

        try:
            univ = VarMgr.get('universite')
        except KeyError:
            raise AirflowException("La variable 'univ' doit être définie pour initialiser AMUETableVerifier")
        try:
            endpointadm = VarMgr.get('api_endpoint_admin')
        except KeyError:
            raise AirflowException(
                "La variable 'api_endpoint_admin' doit être définie pour initialiser AMUETableVerifier")
        try:
            self.endpoint = Template(endpointadm).substitute(univ=univ)
        except KeyError as e:
            raise AirflowException(f"Erreur lors de la substitution dans l'endpoint: {e}")

        if target_schema:
            logger.info(f"[VERIFY] Schéma cible blue/green: {target_schema}")

    def verify_status(self, table_info: Dict) -> Dict:
        """Vérifie le statut d'une table"""
        table_name = table_info.get('name', 'unknown')
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
                'details': current_status
            }

        logger.info(f"[STATUS_CHECK] {table_name}: OK")
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

        Calcule deux fingerprints :
        - fingerprint_API : structure originale API + PKs API (toujours fetchées)
        - fingerprint_UL : structure transformée PG + PKs variable Airflow (fallback API)
        """
        table_name = table_info.get('name', 'unknown')
        logger.info(f"[STRUCTURE_CHECK] Vérification structure: {table_name}")

        logger.info(f"[STRUCTURE_CHECK] table_info keys: {list(table_info.keys())}")
        logger.info(f"[STRUCTURE_CHECK] table_info['primary_key'] = '{table_info.get('primary_key', 'NOT_SET')}'")

        try:
            # Récupère la structure depuis l'API
            columns = self._fetch_structure(table_name)

            # TOUJOURS récupérer les PKs depuis l'API
            api_pks = self._fetch_primary_keys(table_name)
            logger.info(f"[STRUCTURE_CHECK] PKs API: '{api_pks}'")

            # Lire les PKs depuis la config (variable Airflow)
            config_pks = table_info.get('primary_key', '')
            logger.info(f"[STRUCTURE_CHECK] PKs config: '{config_pks}'")

            # Si pas de PKs en config, sauvegarder celles de l'API
            if not config_pks and api_pks:
                logger.info(f"[STRUCTURE_CHECK] => Appel _save_primary_keys({table_name}, {api_pks})")
                self._save_primary_keys(table_name, api_pks)

            if not api_pks and not config_pks:
                logger.warning(f"[WARN] Aucune clé primaire trouvée pour {table_name}")

            # Calcul des deux fingerprints
            fingerprint_API = compute_structure_hash_with_pk(columns, api_pks, type_key='type_original')
            fingerprint_UL = compute_structure_hash_with_pk(columns, config_pks or api_pks, type_key='type_postgres')

            # PKs effectives pour l'import (config prioritaire sur API)
            primary_keys = config_pks or api_pks

            # Vérifie l'existence de la table
            exists = self._table_exists(table_name)

            # Vérifie les changements de structure
            fp_changes = _check_fingerprint_changes(
                table_name,
                fingerprint_API, table_info.get('fingerprint_API', ''),
                fingerprint_UL, table_info.get('fingerprint_UL', ''),
                exists
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
                'fingerprint_UL': fingerprint_UL,
                'primary_keys': primary_keys,
                'exists': exists,
                'structure_changed': structure_changed,
            }

        except Exception as e:
            error_msg = f"Erreur vérification structure {table_name} [{type(e).__name__}]: {e}"
            logger.error(f"[ERROR] {error_msg}")
            return _error_result(table_name, error_msg, [], '', '', '', False, False)

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
        for col_def in self._split_column_defs(columns_def):
            col_def = col_def.strip()
            if not col_def:
                continue

            parts = col_def.split(None, 1)
            if len(parts) >= 2:
                col_name = parts[0].strip().upper()
                if col_name in _META_COLUMNS:
                    continue
                col_type = parts[1].strip().upper()
                pg_type = parse_column_definition(col_type)

                columns.append({
                    'name': col_name,
                    'type_original': col_type,
                    'type_postgres': pg_type
                })

        if not columns:
            raise ValueError("Aucune colonne trouvée")

        return columns

    @staticmethod
    def _split_column_defs(columns_def: str) -> list:
        """
        Découpe la chaîne de définitions de colonnes en respectant les parenthèses.

        Un split naïf sur ',' casse les types avec paramètres comme NUMERIC(15,2).
        Cette méthode ne coupe que les virgules au niveau 0 de parenthèses.

        Exemple :
            "MANDT CHAR(3),WKGBTR NUMERIC(15,2),CODE CHAR(4)"
            → ["MANDT CHAR(3)", "WKGBTR NUMERIC(15,2)", "CODE CHAR(4)"]
        """
        parts = []
        depth = 0
        current = []
        for char in columns_def:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        if current:
            parts.append(''.join(current))
        return parts

    def _fetch_primary_keys(self, table_name: str) -> str:
        """
        Récupère les clés primaires depuis l'API

        NOUVEAU: Logs plus détaillés pour traçabilité
        """
        logger.info(f"[STRUCTURE_CHECK] Appel API pour clés primaires de {table_name}")

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
                logger.warning(f"[WARN] Format de réponse inattendu pour les clés: {type(keys_response)}")
                result = ''

            if result:
                logger.info(f"[STRUCTURE_CHECK] Clés trouvées: {result}")
            else:
                logger.warning(f"[WARN] Aucune clé primaire retournée par l'API")

            return result

        except Exception as e:
            logger.error(f"[ERROR] Erreur lors de la récupération des clés: {str(e)}")
            return ''

    def _save_primary_keys(self, table_name: str, primary_keys: str) -> None:
        """
        Persiste les clés primaires dans splus_admin.amue_tables.

        Args:
            table_name: Nom de la table
            primary_keys: Clés primaires séparées par virgules
        """
        logger.info(f"[SAVE_PK] Tentative sauvegarde PKs pour {table_name}: '{primary_keys}'")

        if not primary_keys:
            logger.warning(f"[SAVE_PK] PKs vides pour {table_name}, abandon")
            return

        from amue.services.table_config_manager import TableConfigManager
        TableConfigManager().save_primary_keys(table_name, primary_keys)
        logger.info(f"[SAVE_PK] SUCCESS - PKs sauvegardées pour {table_name}")

    def _table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe en base dans le schéma cible"""
        # Détermine le schéma à vérifier
        schema_to_check = self.target_schema if self.target_schema else 'splus'

        check_sql = """
                    SELECT EXISTS (SELECT 1
                                   FROM information_schema.tables
                                   WHERE table_schema = %s
                                     AND table_name = %s)
                    """
        result = self.postgres_hook.get_first(check_sql, parameters=(schema_to_check, table_name.lower(),))
        return result[0] if result else False

    def _fetch_existing_columns(self, table_name: str) -> List[Dict]:
        """
        Récupère les colonnes existantes en base via information_schema.

        Returns:
            Liste de dicts {'name': col_name, 'type_postgres': pg_type}
        """
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
        rows = self.postgres_hook.get_records(
            sql, parameters=(schema_to_check, table_name.lower())
        )
        return [
            {
                'name': row[0].upper(),
                'type_postgres': self._format_pg_type(row[1], row[2], row[3], row[4])
            }
            for row in rows
        ]

    @staticmethod
    def _format_pg_type(data_type: str, char_len, num_prec, num_scale) -> str:
        """
        Reconstruit un type PG lisible depuis les champs information_schema.

        Args:
            data_type: Type de données brut (ex: 'character varying')
            char_len: character_maximum_length
            num_prec: numeric_precision
            num_scale: numeric_scale

        Returns:
            Type formaté (ex: 'VARCHAR(50)', 'NUMERIC(10,2)', 'TIMESTAMP')
        """
        dt = data_type.upper()

        if dt == 'CHARACTER VARYING':
            return f"VARCHAR({char_len})" if char_len else "VARCHAR"
        if dt == 'CHARACTER':
            return f"BPCHAR({char_len})" if char_len else "BPCHAR"
        if dt == 'NUMERIC':
            if num_prec is not None and num_scale is not None:
                return f"NUMERIC({num_prec},{num_scale})"
            if num_prec is not None:
                return f"NUMERIC({num_prec})"
            return "NUMERIC"
        if dt == 'TIMESTAMP WITHOUT TIME ZONE':
            return "TIMESTAMP"
        if dt == 'TIMESTAMP WITH TIME ZONE':
            return "TIMESTAMPTZ"
        if dt == 'DOUBLE PRECISION':
            return "DOUBLE PRECISION"

        return dt

    def _compute_structure_diff(self, table_name: str, new_columns: List[Dict]) -> str:
        """
        Compare colonnes existantes PG vs nouvelles colonnes API et produit un diff lisible.

        Symboles :
            + col_name (TYPE)           -> colonne ajoutée
            - col_name (TYPE)           -> colonne supprimée
            ~ col_name: OLD -> NEW      -> changement de type

        Returns:
            Texte du diff multi-lignes, ou message indiquant un changement de PKs
        """
        try:
            existing = self._fetch_existing_columns(table_name)
        except Exception as e:
            return f"(impossible de calculer le diff: {e})"

        existing_map = {col['name']: col['type_postgres'] for col in existing}
        new_map = {col['name']: col['type_postgres'] for col in new_columns}

        diff_lines = []

        # Colonnes ajoutées (dans new mais pas dans existing)
        for name in new_map:
            if name not in existing_map:
                diff_lines.append(f"  + {name} ({new_map[name]})")

        # Colonnes supprimées (dans existing mais pas dans new)
        for name in existing_map:
            if name not in new_map:
                diff_lines.append(f"  - {name} ({existing_map[name]})")

        # Changements de type
        for name in new_map:
            if name in existing_map and new_map[name] != existing_map[name]:
                diff_lines.append(f"  ~ {name}: {existing_map[name]} -> {new_map[name]}")

        if diff_lines:
            return "Differences:\n" + "\n".join(diff_lines)
        return "Aucune difference de colonnes detectee; le changement provient probablement des cles primaires."

    def verify_table(self, table_info: Dict) -> Dict:
        """
        Vérifie une table : statut + structure + fingerprint

        Combine verify_status et verify_structure en une seule opération.
        Inclut la vérification du fingerprint pour détecter les changements.

        Args:
            table_info: Informations de la table

        Returns:
            Résultat complet de la vérification
        """
        table_name = table_info.get('name', 'unknown')
        logger.info(f"[VERIFY] === Vérification complète: {table_name} ===")

        # 1. Vérification du statut
        status_result = self.verify_status(table_info)
        if status_result.get('status') == 'error':
            return {
                'table_name': table_name,
                'status': 'error',
                'phase': 'status',
                'error': status_result.get('error'),
                'original_info': table_info
            }

        # 2. Vérification de la structure
        structure_result = self.verify_structure(table_info)
        if structure_result.get('status') == 'error':
            return {
                'table_name': table_name,
                'status': 'error',
                'phase': 'structure',
                'error': structure_result.get('error'),
                'original_info': table_info
            }

        # 3. Vérification des fingerprints
        old_fp_api = table_info.get('fingerprint_API', '')
        new_fp_api = structure_result.get('fingerprint_API', '')
        old_fp_ul = table_info.get('fingerprint_UL', '')
        new_fp_ul = structure_result.get('fingerprint_UL', '')

        changes = []
        if old_fp_api and new_fp_api and old_fp_api != new_fp_api:
            changes.append(
                f"fingerprint_API: {old_fp_api[:16]}... -> {new_fp_api[:16]}...\n"
                f"  Cause: L'AMUE a modifie la structure source de la table."
            )
        if old_fp_ul and new_fp_ul and old_fp_ul != new_fp_ul:
            diff_detail = self._compute_structure_diff(
                table_name, structure_result.get('columns', [])
            )
            changes.append(
                f"fingerprint_UL: {old_fp_ul[:16]}... -> {new_fp_ul[:16]}...\n"
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
                'original_info': table_info
            }

        # 4. Succès - prépare les données pour l'import
        logger.info(f"[VERIFY] {table_name}: Toutes les vérifications OK")

        # Met à jour les fingerprints si vides
        updated_info = dict(table_info)
        if not old_fp_api and new_fp_api:
            updated_info['fingerprint_API'] = new_fp_api
            logger.info(f"[VERIFY] fingerprint_API initialisé: {new_fp_api[:16]}...")
        if not old_fp_ul and new_fp_ul:
            updated_info['fingerprint_UL'] = new_fp_ul
            logger.info(f"[VERIFY] fingerprint_UL initialisé: {new_fp_ul[:16]}...")

        return {
            'table_name': table_name,
            'status': 'success',
            'phase': 'complete',
            'error': None,
            'columns': structure_result.get('columns', []),
            'primary_keys': structure_result.get('primary_keys', ''),
            'fingerprint_API': new_fp_api,
            'fingerprint_UL': new_fp_ul,
            'exists': structure_result.get('exists', False),
            'original_info': updated_info
        }
