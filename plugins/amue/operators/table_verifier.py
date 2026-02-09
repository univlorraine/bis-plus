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
    - environment : "dev" ou "production"

================================================================================
"""
import logging
from string import Template
from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from amue.exceptions import AMUESchemaError, AMUEStructureChangedError, AMUETableNotFoundError
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.hooks import create_postgres_hook
from amue.utils.transformers import compute_structure_hash_with_pk, parse_column_definition

logger = logging.getLogger(__name__)


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
        logger.info(f"[STRUCTURE_CHECK] {table_name}: Changement détecté")
        logger.info(f"  Ancien fingerprint: {old_fingerprint[:16]}...")
        logger.info(f"  Nouveau fingerprint: {new_fingerprint[:16]}...")

    return changed


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

        self.environment = VarMgr.get('environment', default='production')
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
            error_msg = f"Table {table_name} status={status} (attendu: OK)"
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

        NOUVEAU:
        - Récupère automatiquement les clés primaires si absentes
        - Calcule le fingerprint avec les clés primaires
        """
        table_name = table_info.get('name', 'unknown')
        logger.info(f"[STRUCTURE_CHECK] Vérification structure: {table_name}")

        # DEBUG: Afficher toutes les clés de table_info
        logger.info(f"[STRUCTURE_CHECK] table_info keys: {list(table_info.keys())}")
        logger.info(f"[STRUCTURE_CHECK] table_info['primary_key'] = '{table_info.get('primary_key', 'NOT_SET')}'")

        try:
            # Récupère la structure depuis l'API
            columns = self._fetch_structure(table_name)

            # Récupère les clés primaires
            primary_keys = table_info.get('primary_key', '')

            logger.info(f"[STRUCTURE_CHECK] primary_keys='{primary_keys}'")

            if not primary_keys:
                logger.info(f"[STRUCTURE_CHECK] => Récupération PKs depuis API...")
                primary_keys = self._fetch_primary_keys(table_name)
                logger.info(f"[STRUCTURE_CHECK] PKs retournées par API: '{primary_keys}'")

                if primary_keys:
                    logger.info(f"[STRUCTURE_CHECK] => Appel _save_primary_keys({table_name}, {primary_keys})")
                    # Persister immédiatement dans la variable Airflow
                    self._save_primary_keys(table_name, primary_keys)
                else:
                    logger.warning(f"[WARN] Aucune clé primaire trouvée pour {table_name}")
            else:
                logger.info(f"[STRUCTURE_CHECK] => PKs déjà présentes, pas de fetch: {primary_keys}")

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
                logger.error(f"[ERROR] {error_msg}")
                return _error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, True)

            if not exists and self.environment == 'production':
                error_msg = f"Table {table_name} n'existe pas en production"
                logger.error(f"[ERROR] {error_msg}")
                return _error_result(table_name, error_msg, columns, finger_print, primary_keys, exists, False)

            logger.info(f"[STRUCTURE_CHECK] {table_name}: OK")
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
            }

        except Exception as e:
            error_msg = f"Erreur vérification structure {table_name}: {e}"
            logger.error(f"[ERROR] {error_msg}")
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
        Persiste les clés primaires dans la variable Airflow immédiatement.

        Cette méthode met à jour la variable amue_tables_to_import pour stocker
        les PKs récupérées depuis l'API, permettant ainsi à l'importeur de les
        utiliser sans les passer en paramètre.

        Args:
            table_name: Nom de la table
            primary_keys: Clés primaires séparées par virgules
        """
        logger.info(f"[SAVE_PK] Tentative sauvegarde PKs pour {table_name}: '{primary_keys}'")

        if not primary_keys:
            logger.warning(f"[SAVE_PK] PKs vides pour {table_name}, abandon")
            return

        try:
            tables_var = VarMgr.get('amue_tables_to_import', default='[]')
            logger.info(f"[SAVE_PK] Variable chargée, type={type(tables_var).__name__}")

            tables_config = json.loads(tables_var) if isinstance(tables_var, str) else tables_var
            logger.info(f"[SAVE_PK] Config parsée: {len(tables_config)} tables")

            # Debug: lister les noms de tables dans la config
            config_names = [t.get('name', 'NO_NAME') for t in tables_config if isinstance(t, dict)]
            logger.info(f"[SAVE_PK] Tables dans config: {config_names}")

            updated = False
            table_found = False
            for table in tables_config:
                config_name = table.get('name', '')
                if config_name.upper() == table_name.upper():
                    table_found = True
                    old_pk = table.get('primary_key', '')
                    logger.info(f"[SAVE_PK] Table trouvée! old_pk='{old_pk}', new_pk='{primary_keys}'")

                    if old_pk != primary_keys:
                        table['primary_key'] = primary_keys
                        updated = True
                        logger.info(f"[SAVE_PK] PKs marquées pour mise à jour: {old_pk} -> {primary_keys}")
                    else:
                        logger.info(f"[SAVE_PK] PKs identiques, pas de mise à jour nécessaire")
                    break

            if not table_found:
                logger.error(f"[SAVE_PK] Table {table_name} NON TROUVÉE dans la config!")
                return

            if updated:
                new_config_json = json.dumps(tables_config)
                logger.info(f"[SAVE_PK] Appel VarMgr.set() avec {len(new_config_json)} caractères")
                success = VarMgr.set('amue_tables_to_import', new_config_json)
                if success:
                    logger.info(f"[SAVE_PK] SUCCESS - Variable Airflow mise à jour pour {table_name}")
                else:
                    logger.error(f"[SAVE_PK] ECHEC - VarMgr.set() a retourné False pour {table_name}")
            else:
                logger.info(f"[SAVE_PK] Pas de mise à jour nécessaire pour {table_name}")

        except Exception as e:
            logger.error(f"[SAVE_PK] EXCEPTION pour {table_name}: {type(e).__name__}: {e}")

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

        # 3. Vérification du fingerprint
        existing_fp = table_info.get('finger_print', '')
        new_fp = structure_result.get('finger_print', '')

        if existing_fp and new_fp and existing_fp != new_fp:
            error_msg = (
                f"CHANGEMENT DE STRUCTURE DÉTECTÉ pour {table_name}\n"
                f"Fingerprint stocké: {existing_fp}\n"
                f"Fingerprint API: {new_fp}\n"
                f"Action requise: Vérifier les changements et mettre à jour manuellement."
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

        # Met à jour le fingerprint si vide
        updated_info = dict(table_info)
        if not existing_fp and new_fp:
            updated_info['finger_print'] = new_fp
            logger.info(f"[VERIFY] Fingerprint initialisé: {new_fp[:16]}...")

        return {
            'table_name': table_name,
            'status': 'success',
            'phase': 'complete',
            'error': None,
            'columns': structure_result.get('columns', []),
            'primary_keys': structure_result.get('primary_keys', ''),
            'finger_print': new_fp,
            'exists': structure_result.get('exists', False),
            'original_info': updated_info
        }
