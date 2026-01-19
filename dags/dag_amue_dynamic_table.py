"""
DAG d'import AMUE - Architecture simplifiée

Structure en 4 phases :
  1. INITIALISATION : Attente API + sélection des tables
  2. VÉRIFICATION   : Validation structure des tables
  3. IMPORT         : Préparation structure + import données
  4. FINALISATION   : Métadonnées et rapport
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from typing import List, Dict
from amue import (
    AMUEAPIHook,
    AMUEStatusChecker,
    AMUETableFilter,
    AMUETableVerifier,
    AMUETableManager,
    AMUEDataImporter,
    AMUEPollingService,
    AMUEMetadataManager,
    AMUEReportGenerator,
    send_failure_notification,
    AirflowVariableManager as VarMgr,
)
import json
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id='amue_multi_table_import',
    description='Import AMUE - Architecture simplifiée',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['amue', 'production'],
    on_failure_callback=send_failure_notification,
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_notification,
    }
)
def amue_multi_table_import():
    """
    DAG principal d'import AMUE
    """

    # =========================================================================
    # PHASE 1 : INITIALISATION
    # =========================================================================

    @task(task_id='wait_for_api_and_select')
    def wait_for_api_and_select() -> List[Dict]:
        """
        Attend que l'API soit prête et sélectionne les tables à importer

        1. Polling jusqu'à disponibilité de l'API
        2. Récupération du statut actuel
        3. Filtrage des tables configurées
        4. Stocke les infos de polling dans une variable
        5. Retourne la liste des tables (pour le mapping dynamique)
        """
        api_hook = AMUEAPIHook()
        status_checker = AMUEStatusChecker(api_hook)
        polling_service = AMUEPollingService(status_checker)

        # Phase 1: Attente API
        logger.info("[INIT] Attente disponibilité API...")
        polling_result = polling_service.wait_for_ready()

        # Stocke les infos de polling pour le rapport final
        VarMgr.set('_current_run_polling', json.dumps(polling_result, default=str))

        # Phase 2: Sélection des tables
        logger.info("[INIT] API prête, sélection des tables...")
        current_status = status_checker.get_current_status()

        table_filter = AMUETableFilter()
        tables = table_filter.filter_tables(current_status)

        if not tables:
            logger.info("[INIT] Aucune table à importer")
        else:
            logger.info(f"[INIT] {len(tables)} table(s) à importer")
            for t in tables:
                logger.info(f"  - {t.get('name')}")

        # Retourne uniquement la liste des tables (pour .expand())
        return tables

    # =========================================================================
    # PHASE 2 : VÉRIFICATION
    # =========================================================================

    @task(task_id='verify_table')
    def verify_table(table_info: Dict) -> Dict:
        """
        Vérifie une table : statut + structure + fingerprint
        """
        api_hook = AMUEAPIHook()
        verifier = AMUETableVerifier(api_hook)
        return verifier.verify_table(table_info)

    @task(task_id='validate_tables')
    def validate_tables(verification_results: List[Dict]) -> List[Dict]:
        """
        Valide les résultats des vérifications

        Arrête le DAG si une table a échoué la vérification.
        """
        errors = []
        validated = []

        for result in verification_results:
            table_name = result.get('table_name', 'unknown')

            if result.get('status') == 'error':
                errors.append({
                    'table': table_name,
                    'phase': result.get('phase', 'unknown'),
                    'error': result.get('error')
                })
            else:
                validated.append(result)

        if errors:
            logger.error(f"[VALIDATE] {len(errors)} erreur(s) détectée(s)")
            for err in errors:
                logger.error(f"  {err['table']} ({err['phase']}): {err['error']}")
            raise AirflowException(f"Validation échouée: {len(errors)} table(s) en erreur")

        logger.info(f"[VALIDATE] {len(validated)} table(s) validée(s)")
        return validated

    # =========================================================================
    # PHASE 3 : IMPORT
    # =========================================================================

    @task(task_id='prepare_table')
    def prepare_table(verified_table: Dict) -> Dict:
        """
        Prépare la structure PostgreSQL
        """
        manager = AMUETableManager()
        result = manager.manage_table(verified_table)
        result['original_info'] = verified_table.get('original_info', {})
        return result

    @task(task_id='import_data')
    def import_data(prepared_table: Dict) -> Dict:
        """
        Importe les données d'une table
        """
        api_hook = AMUEAPIHook()
        importer = AMUEDataImporter(api_hook)

        return importer.import_table(
            table_name=prepared_table['table_name'],
            columns=prepared_table['columns'],
            primary_keys=[pk.strip() for pk in prepared_table['primary_keys'].split(',') if pk.strip()],
            import_config=prepared_table['original_info']
        )

    # =========================================================================
    # PHASE 4 : FINALISATION
    # =========================================================================

    @task(task_id='save_metadata')
    def save_metadata(import_results: List[Dict]) -> None:
        """
        Met à jour les métadonnées
        """
        manager = AMUEMetadataManager()
        manager.update_metadata(import_results)
        logger.info(f"[METADATA] Métadonnées mises à jour pour {len(import_results)} table(s)")

    @task(task_id='send_report')
    def send_report(import_results: List[Dict]) -> Dict:
        """
        Génère et envoie le rapport final
        """
        # Récupère les infos de polling depuis la variable
        polling_json = VarMgr.get('_current_run_polling', default='{}')
        try:
            polling_result = json.loads(polling_json)
        except Exception:
            polling_result = {}

        generator = AMUEReportGenerator()
        return generator.generate_and_send(import_results, polling_result)

    # =========================================================================
    # DÉFINITION DU WORKFLOW
    # =========================================================================

    # Phase 1 : Initialisation (retourne la liste des tables)
    tables = wait_for_api_and_select()

    # Phase 2 : Vérification (mapping dynamique sur les tables)
    verifications = verify_table.expand(table_info=tables)
    validated = validate_tables(verifications)

    # Phase 3 : Import
    prepared = prepare_table.expand(verified_table=validated)
    imported = import_data.expand(prepared_table=prepared)

    # Phase 4 : Finalisation
    metadata = save_metadata(imported)
    report = send_report(imported)

    # Dépendances finales
    metadata >> report


# Instanciation du DAG
amue_import_dag = amue_multi_table_import()
