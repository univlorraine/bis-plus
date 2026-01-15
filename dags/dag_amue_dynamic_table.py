"""
DAG d'import AMUE - Architecture simplifiée

Structure en 5 phases :
  1. PRÉPARATION   : Vérification historique
  2. ATTENTE API   : Polling jusqu'à disponibilité
  3. VÉRIFICATION  : Sélection et validation des tables
  4. IMPORT        : Préparation structure + import données
  5. FINALISATION  : Métadonnées et rapport
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
    AirflowVariableManager as VarMngr,
)
from amue.utils.logger import get_logger

logger = get_logger('dag')


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

    Workflow :
    ┌─────────────────┐
    │  check_history  │ ─────────────────────────────┐
    └────────┬────────┘                              │
             │                                       │
             ▼                                       ▼
    ┌─────────────────┐                     ┌───────────────┐
    │  wait_for_api   │ ───────────────────▶│ select_tables │
    └─────────────────┘                     └───────┬───────┘
                                                    │
             ┌──────────────────────────────────────┘
             ▼
    ┌─────────────────┐
    │ verify_table ×N │
    └────────┬────────┘
             ▼
    ┌─────────────────┐
    │ validate_tables │
    └────────┬────────┘
             ▼
    ┌─────────────────┐
    │ prepare_table ×N│
    └────────┬────────┘
             ▼
    ┌─────────────────┐
    │ import_data ×N  │
    └────────┬────────┘
             │
      ┌──────┴──────┐
      ▼             ▼
    ┌─────────┐  ┌─────────────┐
    │ save_   │  │ send_report │
    │ metadata│──│             │
    └─────────┘  └─────────────┘
    """

    # =========================================================================
    # PHASE 1 : PRÉPARATION
    # =========================================================================

    @task(task_id='check_history')
    def check_history() -> Dict:
        """
        Vérifie les statuts des N derniers jours

        Retourne l'historique des statuts pour déterminer
        quelles tables nécessitent un import.
        """
        api_hook = AMUEAPIHook()
        status_checker = AMUEStatusChecker(api_hook)
        max_days = int(VarMngr.get('amue_max_history_days', default='7'))

        logger.info(f"[HISTORY] Vérification des {max_days} derniers jours")
        return status_checker.check_historical_status(max_days)

    # =========================================================================
    # PHASE 2 : ATTENTE API
    # =========================================================================

    @task(task_id='wait_for_api')
    def wait_for_api() -> Dict:
        """
        Attend que l'API soit prête

        Utilise un polling intelligent avec backoff optionnel.
        Timeout configurable via amue_max_wait_hours.
        """
        api_hook = AMUEAPIHook()
        status_checker = AMUEStatusChecker(api_hook)
        polling_service = AMUEPollingService(status_checker)

        logger.info("[POLLING] Attente disponibilité API...")
        return polling_service.wait_for_ready()

    # =========================================================================
    # PHASE 3 : SÉLECTION & VÉRIFICATION
    # =========================================================================

    @task(task_id='select_tables')
    def select_tables(history_result: Dict) -> List[Dict]:
        """
        Sélectionne les tables à importer

        Filtre selon :
        - Tables configurées dans amue_tables_to_import
        - Statut actuel de l'API
        - Historique des imports
        """
        api_hook = AMUEAPIHook()
        status_checker = AMUEStatusChecker(api_hook)
        current_status = status_checker.get_current_status()

        table_filter = AMUETableFilter()
        tables = table_filter.filter_tables(current_status, history_result)

        if not tables:
            logger.info("[SELECT] Aucune table à importer")
        else:
            logger.info(f"[SELECT] {len(tables)} table(s) à importer")
            for t in tables:
                logger.info(f"  - {t.get('name')}")

        return tables

    @task(task_id='verify_table')
    def verify_table(table_info: Dict) -> Dict:
        """
        Vérifie une table : statut + structure + fingerprint

        Vérifications effectuées :
        - Statut de la table dans l'API (doit être OK)
        - Structure de la table (colonnes, types)
        - Fingerprint (détection des changements)
        """
        api_hook = AMUEAPIHook()
        verifier = AMUETableVerifier(api_hook)
        return verifier.verify_table(table_info)

    @task(task_id='validate_tables')
    def validate_tables(verification_results: List[Dict]) -> List[Dict]:
        """
        Valide les résultats des vérifications

        Arrête le DAG si une table a échoué la vérification.
        Retourne la liste des tables validées.
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

        # Si erreurs, on arrête
        if errors:
            logger.error(f"[VALIDATE] {len(errors)} erreur(s) détectée(s)")
            for err in errors:
                logger.error(f"  {err['table']} ({err['phase']}): {err['error']}")
            raise AirflowException(f"Validation échouée: {len(errors)} table(s) en erreur")

        logger.info(f"[VALIDATE] {len(validated)} table(s) validée(s)")
        return validated

    # =========================================================================
    # PHASE 4 : IMPORT
    # =========================================================================

    @task(task_id='prepare_table')
    def prepare_table(verified_table: Dict) -> Dict:
        """
        Prépare la structure PostgreSQL

        En dev : crée la table si elle n'existe pas
        En prod : vérifie que la table existe
        """
        manager = AMUETableManager()
        result = manager.manage_table(verified_table)

        # Propage les infos pour l'import
        result['original_info'] = verified_table.get('original_info', {})
        return result

    @task(task_id='import_data')
    def import_data(prepared_table: Dict) -> Dict:
        """
        Importe les données d'une table

        Utilise INSERT (première fois) ou UPSERT (mises à jour).
        Pagination automatique pour les grands volumes.
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
    # PHASE 5 : FINALISATION
    # =========================================================================

    @task(task_id='save_metadata')
    def save_metadata(import_results: List[Dict]) -> None:
        """
        Met à jour les métadonnées

        Sauvegarde :
        - Date du dernier import par table
        - Fingerprint de la structure
        """
        manager = AMUEMetadataManager()
        manager.update_metadata(import_results)
        logger.info(f"[METADATA] Métadonnées mises à jour pour {len(import_results)} table(s)")

    @task(task_id='send_report')
    def send_report(import_results: List[Dict], history_result: Dict,
                    polling_result: Dict) -> Dict:
        """
        Génère et envoie le rapport final

        Inclut :
        - Résumé de l'exécution
        - Détail par table
        - Envoi email aux destinataires
        """
        generator = AMUEReportGenerator()
        return generator.generate_and_send(import_results, history_result, polling_result)

    # =========================================================================
    # DÉFINITION DU WORKFLOW
    # =========================================================================

    # Phase 1 : Préparation
    history = check_history()

    # Phase 2 : Attente API (dépend de history via >>)
    polling = wait_for_api()
    history >> polling  # Dépendance explicite

    # Phase 3 : Sélection et vérification
    tables = select_tables(history)  # Utilise history pour le filtrage
    polling >> tables  # Dépendance explicite : attend que l'API soit prête

    verifications = verify_table.expand(table_info=tables)
    validated = validate_tables(verifications)

    # Phase 4 : Import
    prepared = prepare_table.expand(verified_table=validated)
    imported = import_data.expand(prepared_table=prepared)

    # Phase 5 : Finalisation
    metadata = save_metadata(imported)
    report = send_report(imported, history, polling)

    # Dépendances finales
    metadata >> report


# Instanciation du DAG
amue_import_dag = amue_multi_table_import()
