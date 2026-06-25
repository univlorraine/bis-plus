"""
Layer: application

Orchestrateur de setup des tables AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module extrait la logique métier de la @task `setup_table` pour la rendre
testable indépendamment d'Airflow.

Séquence orchestrée :
    1. Récupère la structure depuis l'API (colonnes, types, PKs)
    2. Calcule les fingerprints (API + UL)
    3. Compare avec les fingerprints stockés :
       - Nouveau   → initialisation, continuer
       - Identique → mise à jour idempotente, continuer
       - Différent → alerte + statut 'blocked'
    4. Si non bloqué : crée la table PostgreSQL si absente
    5. Sauvegarde atomique : fingerprints, PKs, setup_status='ready'

================================================================================
"""
import logging
from typing import Dict

from amue.infrastructure.hooks.amue_api_hook import AMUEAPIHook
from amue.application.table_management.table_verifier import AMUETableVerifier
from amue.application.table_management.table_manager import AMUETableManager
from amue.application.table_config_manager import TableConfigManager

logger = logging.getLogger(__name__)


class TableSetupOrchestrator:
    """
    Orchestre la vérification, la création et la sauvegarde des fingerprints
    pour une table AMUE.

    Sépare la logique métier du contexte Airflow (@task) afin de permettre
    des tests unitaires sans dépendance sur le scheduler.

    Example:
        >>> orch = TableSetupOrchestrator()
        >>> result = orch.run({'name': 'CSKS', 'target_schema': 'splus_blue', ...})
        >>> print(result['status'])  # 'success' | 'blocked' | 'error'
    """

    def __init__(
        self,
        api_hook: AMUEAPIHook = None,
        table_config_manager: TableConfigManager = None,
    ):
        """
        Initialise l'orchestrateur.

        Args:
            api_hook: Hook API AMUE (créé si non fourni)
            table_config_manager: Gestionnaire de config tables (créé si non fourni)
        """
        self._api_hook = api_hook or AMUEAPIHook()
        self._config_manager = table_config_manager or TableConfigManager()

    def run(self, table_info: Dict) -> Dict:
        """
        Exécute le setup complet d'une table.

        Args:
            table_info: Configuration de la table
                        (format TableConfigManager.get_tables_config(),
                        enrichi avec 'target_schema')

        Returns:
            {
                'table_name': str,
                'status': 'success' | 'blocked' | 'error',
                'setup_status': 'ready' | 'blocked' | 'pending',
                'created': bool,
                'columns_count': int,
                'error': str | None,
            }
        """
        table_name = table_info.get('table_name', 'unknown')
        target_schema = table_info.get('target_schema')
        stored_fp_api = table_info.get('fingerprint_API', '')
        stored_fp_local = table_info.get('fingerprint_local', '')

        logger.info(f"[SETUP] Début setup pour {table_name} (schéma: {target_schema})")

        try:
            structure = self._verify_structure(table_info, target_schema)
            if structure.get('status') == 'error':
                return self._error_result(table_name, structure.get('error'))

            new_fp_api = structure['fingerprint_API']
            new_fp_local = structure['fingerprint_local']
            primary_keys = structure['primary_keys']
            columns = structure['columns']

            if self._structure_changed(stored_fp_api, stored_fp_local, new_fp_api, new_fp_local):
                return self._handle_structure_change(
                    table_name, stored_fp_api, stored_fp_local, new_fp_api, new_fp_local, columns,
                    target_schema=target_schema,
                    table_exists=structure.get('exists', False),
                )

            created = self._create_table_if_needed(structure, target_schema)
            self._save_result(table_name, new_fp_api, new_fp_local, primary_keys)

            action = 'créée' if created else 'existante'
            logger.info(f"[SETUP] {table_name}: OK — table {action}, fingerprints sauvegardés")

            return {
                'table_name': table_name,
                'status': 'success',
                'setup_status': 'ready',
                'created': created,
                'columns_count': len(columns),
                'error': None,
            }

        except Exception as e:
            error_msg = f"[{type(e).__name__}] Erreur setup {table_name}: {e}"
            logger.error(f"[SETUP] {error_msg}")
            return self._error_result(table_name, error_msg)

    # -------------------------------------------------------------------------
    # Méthodes privées
    # -------------------------------------------------------------------------

    def _verify_structure(self, table_info: Dict, target_schema: str) -> Dict:
        verifier = AMUETableVerifier(self._api_hook, target_schema=target_schema)
        return verifier.verify_structure(table_info)

    @staticmethod
    def _structure_changed(
        stored_fp_api: str, stored_fp_local: str,
        new_fp_api: str, new_fp_local: str,
    ) -> bool:
        is_new = not stored_fp_api and not stored_fp_local
        return (
            not is_new
            and (stored_fp_api != new_fp_api or stored_fp_local != new_fp_local)
        )

    def _handle_structure_change(
        self,
        table_name: str,
        stored_fp_api: str, stored_fp_local: str,
        new_fp_api: str, new_fp_local: str,
        columns,
        target_schema: str = None,
        table_exists: bool = False,
    ) -> Dict:
        fp_api_changed = stored_fp_api != new_fp_api
        fp_local_changed = stored_fp_local != new_fp_local

        # Calcul du diff de colonnes si le fingerprint_local a changé et que la table existe en PG
        ul_diff = None
        if fp_local_changed and table_exists and target_schema:
            try:
                verifier = AMUETableVerifier(self._api_hook, target_schema=target_schema)
                ul_diff = verifier._compute_structure_diff(table_name, columns)
            except Exception as diff_err:
                ul_diff = f"(impossible de calculer le diff: {diff_err})"

        error_msg = (
            f"[SETUP] Changement de structure détecté pour {table_name} :\n"
            f"  fingerprint_API : {stored_fp_api[:16]}... → {new_fp_api[:16]}...\n"
            f"  fingerprint_local  : {stored_fp_local[:16]}... → {new_fp_local[:16]}..."
        )
        logger.error(error_msg)
        self._config_manager.set_setup_status(table_name, 'blocked')

        return {
            'table_name': table_name,
            'status': 'blocked',
            'setup_status': 'blocked',
            'created': False,
            'columns_count': len(columns),
            'error': error_msg,
            'fp_api_changed': fp_api_changed,
            'fp_local_changed': fp_local_changed,
            'ul_diff': ul_diff,
        }

    @staticmethod
    def _create_table_if_needed(structure: Dict, target_schema: str) -> bool:
        manager = AMUETableManager(target_schema=target_schema)
        result = manager.manage_table(structure)
        return result.get('created', False)

    def _save_result(
        self, table_name: str, fp_api: str, fp_local: str, primary_keys: str
    ) -> None:
        self._config_manager.save_setup_result(
            table_name=table_name,
            fingerprint_api=fp_api,
            fingerprint_local=fp_local,
            primary_keys=primary_keys,
        )

    @staticmethod
    def _error_result(table_name: str, error: str) -> Dict:
        return {
            'table_name': table_name,
            'status': 'error',
            'setup_status': 'pending',
            'created': False,
            'columns_count': 0,
            'error': error,
        }
