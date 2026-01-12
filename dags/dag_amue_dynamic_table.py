"""
DAG refactorisé pour l'import AMUE
Architecture propre avec séparation des responsabilités
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task, Variable
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
    send_failure_notification
)


@dag(
    dag_id='amue_multi_table_import',
    description='Import AMUE avec architecture refactorisée',
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
def amue_multi_table_import_v2():
    """DAG principal d'import AMUE"""

    # ========================================================================
    # VÉRIFICATION HISTORIQUE
    # ========================================================================

    @task
    def check_historical_status() -> Dict:
        """Vérifie les statuts historiques"""
        api_hook = AMUEAPIHook()

        status_checker = AMUEStatusChecker(api_hook)
        max_days = int(Variable.get('amue_max_history_days', default='7'))

        return status_checker.check_historical_status(max_days)

    # ========================================================================
    # POLLING
    # ========================================================================

    @task
    def wait_for_update_ready(history_result: Dict) -> Dict:
        """Attend que l'API soit prête"""
        api_hook = AMUEAPIHook()

        status_checker = AMUEStatusChecker(api_hook)
        polling_service = AMUEPollingService(status_checker)

        return polling_service.wait_for_ready()

    # ========================================================================
    # FILTRAGE DES TABLES
    # ========================================================================

    @task
    def filter_tables_to_process(polling_result: Dict, history_result: Dict) -> List[Dict]:
        """Filtre les tables à traiter"""
        api_hook = AMUEAPIHook()

        status_checker = AMUEStatusChecker(api_hook)
        current_status = status_checker.get_current_status()

        table_filter = AMUETableFilter()
        tables_to_process = table_filter.filter_tables(current_status, history_result)

        if not tables_to_process:
            print("[FILTER] Aucune table à traiter")

        return tables_to_process

    # ========================================================================
    # VÉRIFICATIONS PARALLÈLES
    # ========================================================================

    @task(task_id='verify_status')
    def verify_table_status(table_info: Dict) -> Dict:
        """Vérifie le statut d'une table"""
        api_hook = AMUEAPIHook()

        verifier = AMUETableVerifier(api_hook)
        return verifier.verify_status(table_info)

    @task(task_id='verify_structure')
    def verify_table_structure(table_info: Dict) -> Dict:
        """Vérifie la structure d'une table"""
        api_hook = AMUEAPIHook()

        verifier = AMUETableVerifier(api_hook)
        return verifier.verify_structure(table_info)

    # ========================================================================
    # COMBINAISON DES VÉRIFICATIONS
    # ========================================================================

    @task
    def combine_verifications(status_checks: List[Dict], structure_checks: List[Dict],
                             tables_list: List[Dict]) -> List[Dict]:
        """Combine les vérifications et prépare pour l'import"""
        print("[COMBINE] Combinaison des vérifications")

        # Indexe par nom de table
        status_map = {s['table_name']: s for s in status_checks}
        structure_map = {s['table_name']: s for s in structure_checks}
        tables_map = {t['name'].upper(): t for t in tables_list}

        # Vérifie les erreurs
        errors = []
        tables_ready = []

        for table_name in status_map.keys():
            status = status_map[table_name]
            structure = structure_map.get(table_name, {})
            original = tables_map.get(table_name, {})

            # Collecte les erreurs
            if status.get('status') == 'error':
                errors.append({
                    'table': table_name,
                    'type': 'status',
                    'error': status.get('error')
                })

            if structure.get('status') == 'error':
                errors.append({
                    'table': table_name,
                    'type': 'structure',
                    'error': structure.get('error')
                })

            # Si tout est OK, prépare pour l'import
            if status.get('status') == 'success' and structure.get('status') == 'success':
                tables_ready.append({
                    'structure_info': structure,
                    'original_info': original
                })

        # Si erreurs, on arrête
        if errors:
            error_msg = f"Erreurs détectées: {len(errors)} problème(s)"
            for err in errors:
                print(f"[ERROR] {err['table']}: {err['error']}")
            raise AirflowException(error_msg)

        print(f"[COMBINE] {len(tables_ready)} tables prêtes")
        return tables_ready

    # ========================================================================
    # GESTION DES STRUCTURES
    # ========================================================================

    @task(task_id='manage_structure')
    def manage_table_structure(table_ready: Dict) -> Dict:
        """Gère la structure d'une table"""
        manager = AMUETableManager()
        result = manager.manage_table(table_ready['structure_info'])

        # Ajoute les infos originales pour l'import
        result['original_info'] = table_ready['original_info']
        return result

    # ========================================================================
    # IMPORT DES DONNÉES
    # ========================================================================

    @task(task_id='import_data')
    def import_table_data(table_mgmt: Dict) -> Dict:
        """Importe les données d'une table"""
        api_hook = AMUEAPIHook()

        importer = AMUEDataImporter(api_hook)

        return importer.import_table(
            table_name=table_mgmt['table_name'],
            columns=table_mgmt['columns'],
            primary_keys=[pk.strip() for pk in table_mgmt['primary_keys'].split(',') if pk.strip()],
            import_config=table_mgmt['original_info']
        )

    # ========================================================================
    # MÉTADONNÉES ET RAPPORTS
    # ========================================================================

    @task
    def update_metadata(import_results: List[Dict]) -> None:
        """Met à jour les métadonnées"""
        manager = AMUEMetadataManager()
        manager.update_metadata(import_results)

    @task
    def generate_report(insert_results: List[Dict], history_result: Dict,
                       polling_result: Dict) -> Dict:
        """Génère le rapport d'exécution"""
        generator = AMUEReportGenerator()
        return generator.generate_report(insert_results, history_result, polling_result)

    @task
    def send_notification(report: Dict) -> None:
        """Envoie la notification"""
        generator = AMUEReportGenerator()
        generator.send_notification(report)

    # ========================================================================
    # DÉFINITION DU WORKFLOW
    # ========================================================================

    # 1. Historique et polling
    history = check_historical_status()
    polling = wait_for_update_ready(history)

    # 2. Filtrage
    tables_to_process = filter_tables_to_process(polling, history)

    # 3. Vérifications parallèles
    status_checks = verify_table_status.expand(table_info=tables_to_process)
    structure_checks = verify_table_structure.expand(table_info=tables_to_process)

    # 4. Combinaison
    tables_ready = combine_verifications(status_checks, structure_checks, tables_to_process)

    # 5. Gestion structures
    table_mgmts = manage_table_structure.expand(table_ready=tables_ready)

    # 6. Import
    import_results = import_table_data.expand(table_mgmt=table_mgmts)

    # 7. Finalisation
    metadata = update_metadata(import_results)
    report = generate_report(import_results, history, polling)
    notification = send_notification(report)

    # Dépendances finales
    metadata >> report >> notification


# Instanciation du DAG
amue_import_dag = amue_multi_table_import_v2()