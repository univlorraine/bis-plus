"""
DAG de Rollback Blue/Green pour l'import AMUE

Ce DAG permet de revenir rapidement à l'état précédent en cas de
problème avec les données importées. Le rollback est instantané car
il consiste simplement à reswitcher les vues vers l'ancien schéma.

================================================================================
QUAND UTILISER CE DAG
================================================================================

Utilisez ce DAG quand :
    - Les données importées sont incorrectes
    - Un problème est détecté après un import réussi
    - Vous souhaitez revenir à l'état précédent

CONDITIONS :
    - Le mode blue/green doit être activé
    - Un import doit avoir été effectué depuis la dernière sync
    - Aucun import ne doit être en cours

================================================================================
COMMENT DÉCLENCHER
================================================================================

1. Via Airflow CLI :
   airflow dags trigger amue_rollback

2. Via API Airflow :
   POST /api/v1/dags/amue_rollback/dagRuns

3. Via UI Airflow :
   Aller dans DAGs > amue_rollback > Trigger DAG

================================================================================
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from typing import Dict

from amue.services.bluegreen_manager import BlueGreenManager
from amue.services.rollback_manager import RollbackManager
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue import send_failure_notification

import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id='amue_rollback',
    description='Rollback Blue/Green - Retour à l\'état précédent',

    # --- Planification ---
    schedule=None,                  # Déclenché manuellement uniquement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,              # Un seul rollback à la fois

    # --- Métadonnées ---
    tags=['amue', 'bluegreen', 'rollback', 'manual'],

    # --- Gestion des erreurs ---
    on_failure_callback=send_failure_notification,

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'on_failure_callback': send_failure_notification,
    }
)
def amue_rollback():
    """
    DAG de rollback blue/green

    Workflow :
        check_rollback_available()
            ↓
        preview_rollback()
            ↓
        execute_rollback()
            ↓
        verify_rollback()
    """

    @task(task_id='check_rollback_available')
    def check_rollback_available() -> Dict:
        """
        Vérifie que le rollback est possible.

        Conditions vérifiées :
            - Mode blue/green activé
            - Pas d'import en cours
            - Rollback disponible (pas encore sync)

        Returns:
            Informations sur le rollback disponible

        Raises:
            AirflowException: Si rollback impossible
        """
        manager = RollbackManager()
        info = manager.get_rollback_info()

        if not info.get('available'):
            reason = info.get('reason', 'Raison inconnue')
            logger.error(f"[ROLLBACK] Non disponible: {reason}")
            raise AirflowException(f"Rollback impossible: {reason}")

        logger.info(f"[ROLLBACK] Disponible")
        logger.info(f"[ROLLBACK] Schéma actuel: {info.get('current_schema')}")
        logger.info(f"[ROLLBACK] Schéma de rollback: {info.get('rollback_schema')}")
        logger.info(f"[ROLLBACK] Dernier switch: {info.get('last_switch')}")

        return info

    @task(task_id='preview_rollback')
    def preview_rollback(check_result: Dict) -> Dict:
        """
        Prévisualise le rollback avant exécution.

        Affiche les informations sur ce qui va se passer.

        Args:
            check_result: Résultat de check_rollback_available()

        Returns:
            Prévisualisation du rollback
        """
        manager = RollbackManager()
        preview = manager.preview_rollback()

        logger.info("[ROLLBACK] === Prévisualisation ===")
        logger.info(f"[ROLLBACK] De: {preview.get('from_schema')}")
        logger.info(f"[ROLLBACK] Vers: {preview.get('to_schema')}")
        logger.info(f"[ROLLBACK] Dernier switch: {preview.get('last_switch')}")

        return preview

    @task(task_id='execute_rollback')
    def execute_rollback(preview_result: Dict) -> Dict:
        """
        Exécute le rollback.

        Switch les vues vers le schéma de rollback.

        Args:
            preview_result: Résultat de preview_rollback()

        Returns:
            Résultat du rollback

        Raises:
            AirflowException: Si le rollback échoue
        """
        manager = RollbackManager()
        result = manager.rollback()

        if not result.get('success'):
            error = result.get('error', 'Erreur inconnue')
            logger.error(f"[ROLLBACK] Échec: {error}")
            raise AirflowException(f"Rollback échoué: {error}")

        logger.info("[ROLLBACK] === Rollback effectué ===")
        logger.info(f"[ROLLBACK] Ancien schéma: {result.get('previous_schema')}")
        logger.info(f"[ROLLBACK] Nouveau schéma: {result.get('new_schema')}")

        return result

    @task(task_id='verify_rollback')
    def verify_rollback(rollback_result: Dict) -> Dict:
        """
        Vérifie que le rollback a réussi.

        Contrôle que les vues pointent vers le bon schéma.

        Args:
            rollback_result: Résultat de execute_rollback()

        Returns:
            Résultat de la vérification

        Raises:
            AirflowException: Si la vérification échoue
        """
        manager = RollbackManager()
        verification = manager.verify_rollback_integrity()

        if not verification.get('verified'):
            logger.error("[ROLLBACK] Vérification échouée!")
            logger.error(f"[ROLLBACK] Schéma attendu: {verification.get('expected_schema')}")
            logger.error(f"[ROLLBACK] Schéma actuel: {verification.get('actual_schema')}")
            raise AirflowException("Vérification du rollback échouée")

        logger.info("[ROLLBACK] === Vérification OK ===")
        logger.info(f"[ROLLBACK] Schéma actif: {verification.get('expected_schema')}")
        logger.info("[ROLLBACK] Toutes les vues pointent vers le bon schéma")

        return {
            'status': 'success',
            'new_active_schema': verification.get('expected_schema'),
            'rollback_from': rollback_result.get('previous_schema'),
            'rollback_to': rollback_result.get('new_schema')
        }

    # Workflow
    check = check_rollback_available()
    preview = preview_rollback(check)
    rollback = execute_rollback(preview)
    verify = verify_rollback(rollback)


# Instanciation du DAG
amue_rollback_dag = amue_rollback()
