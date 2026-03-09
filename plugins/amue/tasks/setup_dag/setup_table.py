"""Task d'initialisation d'une table : vérification, création et sauvegarde des fingerprints."""
import logging
from typing import Dict

from airflow.sdk import task

from amue.hooks.amue_api_hook import AMUEAPIHook
from amue.operators.table_management.table_verifier import AMUETableVerifier
from amue.operators.table_management.table_manager import AMUETableManager
from amue.services.table_config_manager import TableConfigManager
from amue.notifications.notifier import NotificationService

logger = logging.getLogger(__name__)


@task(task_id='setup_table')
def setup_table(table_info: Dict) -> Dict:
    """
    Initialise ou vérifie une table AMUE.

    Cette task est exécutée en parallèle pour chaque table (via .expand()).

    Séquence :
        1. Récupère la structure depuis l'API (colonnes, types, PKs)
        2. Calcule les fingerprints (API + UL)
        3. Compare avec les fingerprints stockés :
           - Nouveau (stocké vide)   → initialisation, continuer
           - Identique               → mise à jour idempotente, continuer
           - Différent               → détection uniquement : alerte + statut 'blocked'
        4. Si non bloqué : crée la table PostgreSQL si absente
        5. Sauvegarde atomique : fingerprints, PKs, setup_status='ready'

    Args:
        table_info: Configuration de la table (format TableConfigManager.get_tables_config())

    Returns:
        Résultat du setup : {table_name, status, setup_status, created, columns_count, error}
    """
    table_name = table_info.get('name', 'unknown')
    target_schema = table_info.get('target_schema')
    stored_fp_api = table_info.get('fingerprint_API', '')
    stored_fp_ul = table_info.get('fingerprint_UL', '')

    logger.info(f"[SETUP] Début setup pour {table_name} (schéma: {target_schema})")

    try:
        api_hook = AMUEAPIHook()
        verifier = AMUETableVerifier(api_hook, target_schema=target_schema)

        structure = verifier.verify_structure(table_info)

        if structure.get('status') == 'error':
            logger.error(f"[SETUP] Erreur vérification structure {table_name}: {structure.get('error')}")
            return {
                'table_name': table_name,
                'status': 'error',
                'setup_status': 'pending',
                'created': False,
                'columns_count': 0,
                'error': structure.get('error'),
            }

        new_fp_api = structure['fingerprint_API']
        new_fp_ul = structure['fingerprint_UL']
        primary_keys = structure['primary_keys']
        columns = structure['columns']

        # Détection de changement de structure
        is_new = not stored_fp_api and not stored_fp_ul
        fp_changed = (
            not is_new
            and (stored_fp_api != new_fp_api or stored_fp_ul != new_fp_ul)
        )

        if fp_changed:
            error_msg = (
                f"[SETUP] Changement de structure détecté pour {table_name} :\n"
                f"  fingerprint_API : {stored_fp_api[:16]}... → {new_fp_api[:16]}...\n"
                f"  fingerprint_UL  : {stored_fp_ul[:16]}... → {new_fp_ul[:16]}..."
            )
            logger.error(error_msg)

            TableConfigManager().set_setup_status(table_name, 'blocked')

            try:
                NotificationService().notify_error({
                    'dag_id': 'amue_table_setup',
                    'task_id': 'setup_table',
                    'error_message': error_msg,
                    'error_type': 'StructureChangeDetected',
                })
            except Exception as notif_err:
                logger.warning(f"[SETUP] Envoi notification échoué: {notif_err}")

            return {
                'table_name': table_name,
                'status': 'blocked',
                'setup_status': 'blocked',
                'created': False,
                'columns_count': len(columns),
                'error': error_msg,
            }

        # Création de la table si nécessaire
        manager = AMUETableManager(target_schema=target_schema)
        mgmt_result = manager.manage_table(structure)
        created = mgmt_result.get('created', False)

        # Sauvegarde atomique du résultat
        TableConfigManager().save_setup_result(
            table_name=table_name,
            fingerprint_api=new_fp_api,
            fingerprint_ul=new_fp_ul,
            primary_keys=primary_keys,
        )

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
        return {
            'table_name': table_name,
            'status': 'error',
            'setup_status': 'pending',
            'created': False,
            'columns_count': 0,
            'error': error_msg,
        }
