"""
DAG d'import AMUE - Import automatisé de données financières universitaires

Ce DAG récupère les données depuis l'API AMUE (Agence de Mutualisation des
Universités et Établissements) et les importe dans une base PostgreSQL.

================================================================================
ARCHITECTURE EN 4 PHASES
================================================================================

PHASE 1 - INITIALISATION
    └── wait_for_api_and_select()
        • Attend que l'API AMUE soit disponible (polling avec backoff)
        • Vérifie que le traitement côté AMUE est terminé (variable 'finish')
        • Sélectionne les tables à importer selon la configuration
        • Retourne la liste des tables pour le traitement parallèle

PHASE 2 - VÉRIFICATION (parallèle, 1 task par table)
    ├── verify_table.expand()
    │   • Vérifie le statut de chaque table côté API
    │   • Compare le fingerprint pour détecter les changements de structure
    │   • Récupère la définition des colonnes
    │
    └── validate_tables()
        • Agrège les résultats de vérification
        • STOPPE le DAG si une table est en erreur (fail-fast)

PHASE 3 - IMPORT (parallèle, 1 task par table)
    ├── prepare_table.expand()
    │   • Crée/modifie la table PostgreSQL si nécessaire (dev uniquement)
    │   • En production : vérifie que la structure existe
    │
    └── import_data.expand()
        • Récupère les données par batch depuis l'API
        • INSERT ou UPSERT selon la présence de clé primaire
        • Gestion des erreurs avec retry intelligent

PHASE 4 - FINALISATION
    ├── save_metadata()
    │   • Met à jour les fingerprints dans les variables Airflow
    │   • Enregistre la date de dernier import par table
    │
    ├── switch_views()
    │   • Bascule atomique des vues vers le schéma cible (blue/green)
    │
    └── send_report()
        • Génère un rapport HTML de l'exécution
        • Envoie par email aux destinataires configurés

================================================================================
CONFIGURATION (Variables Airflow)
================================================================================

Voir plugins/amue/utils/settings.py pour la liste complète des variables.

Variables principales :
    - amue_tables_to_import : Liste JSON des tables à importer
    - environment : "dev" ou "production"
    - universite : Code université (ex: "ul")

================================================================================
PLANIFICATION
================================================================================

Schedule : Tous les jours à 2h00 (0 2 * * *)
Catchup  : Désactivé (pas de rattrapage des exécutions manquées)
Max runs : 1 seul DAG run actif à la fois

================================================================================
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
from amue.services.bluegreen_manager import BlueGreenManager
from amue.services.view_switcher import ViewSwitcher
import json
import logging

logger = logging.getLogger(__name__)


# ==============================================================================
# DÉFINITION DU DAG
# ==============================================================================

@dag(
    dag_id='amue_multi_table_import',
    description='Import AMUE - Architecture simplifiée',

    # --- Planification ---
    schedule='0 2 * * *',           # Tous les jours à 2h00
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # Pas de rattrapage des runs manqués
    max_active_runs=1,              # Un seul run actif à la fois

    # --- Métadonnées ---
    tags=['amue', 'production'],

    # --- Gestion des erreurs ---
    # Envoie un email en cas d'échec du DAG
    on_failure_callback=send_failure_notification,

    # --- Configuration par défaut des tasks ---
    default_args={
        'owner': 'airflow',
        'retries': 0,               # Pas de retry automatique (géré dans le code)
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_notification,
    }
)
def amue_multi_table_import():
    """
    DAG principal d'import AMUE

    Workflow :
        tables = wait_for_api_and_select()
            ↓
        verifications = verify_table.expand(tables)  # Parallèle
            ↓
        validated = validate_tables(verifications)
            ↓
        prepared = prepare_table.expand(validated)   # Parallèle
            ↓
        imported = import_data.expand(prepared)      # Parallèle
            ↓
        save_metadata(imported) >> send_report(imported)
    """

    # ==========================================================================
    # PHASE 0 : BLUE/GREEN INITIALISATION
    # ==========================================================================

    @task(task_id='init_bluegreen')
    def init_bluegreen() -> Dict:
        """
        Initialise le contexte blue/green pour ce DAG run.

        Détermine le schéma cible (opposé de l'actif) et prépare l'état.

        Returns:
            Contexte blue/green :
            {
                "enabled": True/False,
                "target_schema": "splus_green",
                "active_schema": "splus_blue",
                "needs_sync": True/False
            }
        """
        manager = BlueGreenManager()

        if not manager.is_enabled():
            logger.info("[BLUEGREEN] Mode désactivé - import classique")
            return {
                "enabled": False,
                "target_schema": None,
                "active_schema": None,
                "needs_sync": False
            }

        target = manager.get_target_schema()
        active = manager.get_active_schema()
        needs_sync = manager.needs_sync()

        logger.info(f"[BLUEGREEN] Mode activé")
        logger.info(f"[BLUEGREEN] Schéma actif: {active}")
        logger.info(f"[BLUEGREEN] Schéma cible: {target}")
        logger.info(f"[BLUEGREEN] Sync nécessaire: {needs_sync}")

        # Marque le début de l'import
        manager.mark_import_started()

        return {
            "enabled": True,
            "target_schema": target,
            "active_schema": active,
            "needs_sync": needs_sync
        }

    # ==========================================================================
    # PHASE 1 : INITIALISATION
    # ==========================================================================

    @task(task_id='wait_for_api_and_select')
    def wait_for_api_and_select(bluegreen_ctx: Dict) -> List[Dict]:
        """
        Attend la disponibilité de l'API et sélectionne les tables à importer

        Étapes :
            1. Polling de l'API jusqu'à disponibilité (code HTTP 200)
            2. Vérification de la variable 'finish' (traitement AMUE terminé)
            3. Récupération du statut actuel de toutes les tables
            4. Filtrage selon la configuration (amue_tables_to_import)

        Returns:
            Liste de dictionnaires contenant les infos de chaque table :
            [
                {
                    "name": "CSKS",
                    "primary_key": "BUKRS,KOSTL",
                    "delta": "",
                    "finger_print": "abc123...",
                    "last_import": "2024-01-15T10:30:00",
                    "api_status": {"status": "OK", "count": 1500, ...}
                },
                ...
            ]

        Raises:
            AirflowException: Si l'API n'est pas disponible après le timeout
        """
        # Initialisation des services
        api_hook = AMUEAPIHook()                     # Connexion OAuth à l'API
        status_checker = AMUEStatusChecker(api_hook) # Vérificateur de statuts
        polling_service = AMUEPollingService(status_checker)  # Service de polling

        # --- Étape 1 : Attente de l'API ---
        # Le polling vérifie périodiquement si l'API répond (HTTP 200)
        # et si la variable 'finish' est renseignée (traitement AMUE terminé)
        logger.info("[INIT] Attente disponibilité API...")
        polling_result = polling_service.wait_for_ready()

        # Stocke les infos de polling pour le rapport final
        # (durée d'attente, nombre de tentatives, etc.)
        VarMgr.set('_current_run_polling', json.dumps(polling_result, default=str))

        # --- Étape 2 : Sélection des tables ---
        logger.info("[INIT] API prête, sélection des tables...")

        # Utilise le cache tables_status du polling (évite un appel API supplémentaire)
        # Le polling a déjà récupéré cette info lors du dernier appel fetch_full_status()
        current_status = polling_result.get('tables_status', {})
        if not current_status:
            # Fallback si tables_status non disponible (ne devrait pas arriver)
            logger.warning("[INIT] tables_status non disponible, appel API de secours")
            current_status = status_checker.get_current_status()
        else:
            logger.info(f"[INIT] Utilisation du cache tables_status ({len(current_status)} tables)")

        # Filtre les tables selon la configuration Airflow
        # Ne garde que les tables listées dans amue_tables_to_import
        table_filter = AMUETableFilter()
        tables = table_filter.filter_tables(current_status)

        # Injecte le schéma cible blue/green dans chaque table
        target_schema = bluegreen_ctx.get("target_schema") if bluegreen_ctx.get("enabled") else None
        for table in tables:
            table["target_schema"] = target_schema

        # Log des tables sélectionnées
        if not tables:
            logger.info("[INIT] Aucune table à importer")
        else:
            logger.info(f"[INIT] {len(tables)} table(s) à importer")
            if target_schema:
                logger.info(f"[INIT] Schéma cible: {target_schema}")
            for t in tables:
                logger.info(f"  - {t.get('name')}")

        # Retourne la liste pour le mapping dynamique (.expand())
        return tables

    # ==========================================================================
    # PHASE 2 : VÉRIFICATION
    # ==========================================================================

    @task(task_id='verify_table')
    def verify_table(table_info: Dict) -> Dict:
        """
        Vérifie une table avant import

        Cette task est exécutée en parallèle pour chaque table (via .expand()).
        Elle vérifie que :
            - La table est disponible côté API (statut OK)
            - La structure n'a pas changé (comparaison fingerprint)
            - Les colonnes sont valides

        Args:
            table_info: Dictionnaire avec les infos de la table
                {
                    "name": "CSKS",
                    "primary_key": "...",
                    "finger_print": "...",  # Fingerprint stocké
                    "api_status": {...},
                    "target_schema": "splus_blue" | None  # Blue/green
                }

        Returns:
            Résultat de vérification :
            {
                "table_name": "CSKS",
                "status": "success" | "error",
                "phase": "status" | "structure" | "fingerprint",
                "columns": [...],           # Si succès
                "primary_keys": "...",
                "finger_print": "...",      # Nouveau fingerprint
                "original_info": {...},     # Infos originales pour l'import
                "target_schema": "...",     # Schéma cible blue/green
                "error": "..."              # Si erreur
            }
        """
        api_hook = AMUEAPIHook()
        target_schema = table_info.get("target_schema")
        verifier = AMUETableVerifier(api_hook, target_schema=target_schema)
        result = verifier.verify_table(table_info)
        # Propage le schéma cible dans le résultat
        result["target_schema"] = target_schema
        return result

    @task(task_id='validate_tables')
    def validate_tables(verification_results: List[Dict]) -> List[Dict]:
        """
        Valide les résultats de vérification et décide de continuer ou non

        Comportement FAIL-FAST : si une seule table est en erreur,
        le DAG entier s'arrête. Cela évite d'importer partiellement
        les données et de créer des incohérences.

        Args:
            verification_results: Liste des résultats de verify_table()

        Returns:
            Liste des tables validées (status == "success")

        Raises:
            AirflowException: Si au moins une table est en erreur
        """
        errors = []
        validated = []

        # Trie les résultats entre succès et erreurs
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

        # FAIL-FAST : arrête si erreurs détectées
        if errors:
            logger.error(f"[VALIDATE] {len(errors)} erreur(s) détectée(s)")
            for err in errors:
                logger.error(f"  {err['table']} ({err['phase']}): {err['error']}")
            raise AirflowException(f"Validation échouée: {len(errors)} table(s) en erreur")

        logger.info(f"[VALIDATE] {len(validated)} table(s) validée(s)")
        return validated

    # ==========================================================================
    # PHASE 3 : IMPORT
    # ==========================================================================

    @task(task_id='prepare_table')
    def prepare_table(verified_table: Dict) -> Dict:
        """
        Prépare la structure PostgreSQL pour l'import

        En mode DÉVELOPPEMENT (environment=dev) :
            - Crée la table si elle n'existe pas
            - Modifie la structure si le fingerprint a changé

        En mode PRODUCTION (environment=production) :
            - Vérifie que la table existe
            - REFUSE de créer/modifier (sécurité)

        Args:
            verified_table: Résultat de verify_table() avec colonnes et fingerprint

        Returns:
            Dictionnaire enrichi avec les infos pour l'import :
            {
                "table_name": "CSKS",
                "columns": [...],
                "primary_keys": "BUKRS,KOSTL",
                "original_info": {...},  # Config de la table pour l'import
                "target_schema": "..."   # Schéma cible blue/green
            }
        """
        target_schema = verified_table.get("target_schema")
        manager = AMUETableManager(target_schema=target_schema)
        result = manager.manage_table(verified_table)

        # Conserve les infos originales pour l'import
        # (delta, primary_key de la config, etc.)
        result['original_info'] = verified_table.get('original_info', {})
        # Propage le schéma cible
        result['target_schema'] = target_schema
        return result

    @task(task_id='import_data')
    def import_data(prepared_table: Dict) -> Dict:
        """
        Importe les données d'une table depuis l'API vers PostgreSQL

        Stratégie d'import :
            - Si clé primaire définie : UPSERT (INSERT ON CONFLICT UPDATE)
            - Sinon : INSERT simple (truncate + insert)

        Gestion des gros volumes :
            - Import par batch (taille configurable via amue_import_batch_size)
            - Pagination automatique des requêtes API

        Gestion des erreurs :
            - Retry intelligent selon le type d'erreur (4xx, 5xx, timeout)
            - Rollback automatique en cas d'échec

        Blue/Green :
            - Si target_schema spécifié, importe dans ce schéma
            - Sinon, utilise le schéma par défaut (splus)

        Args:
            prepared_table: Résultat de prepare_table()

        Returns:
            Résultat de l'import :
            {
                "table_name": "CSKS",
                "status": "success" | "error",
                "rows_imported": 1500,
                "finger_print": "...",
                "duration_seconds": 45.2,
                "target_schema": "...",  # Schéma où les données ont été importées
                "error": "..."  # Si échec
            }
        """
        api_hook = AMUEAPIHook()
        target_schema = prepared_table.get("target_schema")
        importer = AMUEDataImporter(api_hook, target_schema=target_schema)

        # Parse les clés primaires (string séparé par virgules -> liste)
        primary_keys = [
            pk.strip()
            for pk in prepared_table['primary_keys'].split(',')
            if pk.strip()
        ]

        result = importer.import_table(
            table_name=prepared_table['table_name'],
            columns=prepared_table['columns'],
            primary_keys=primary_keys,
            import_config=prepared_table['original_info']
        )

        # Ajoute le schéma cible au résultat
        result['target_schema'] = target_schema
        return result

    # ==========================================================================
    # PHASE 4 : FINALISATION
    # ==========================================================================

    @task(task_id='save_metadata')
    def save_metadata(import_results: List[Dict]) -> Dict:
        """
        Met à jour les métadonnées après un import réussi

        Pour chaque table importée avec succès :
            - Sauvegarde le nouveau fingerprint
            - Enregistre la date de dernier import
            - Sauvegarde le finish timestamp pour le prochain polling

        Ces métadonnées sont stockées dans la variable Airflow
        'amue_tables_to_import' et servent à :
            - Détecter les changements de structure (fingerprint)
            - Permettre l'import différentiel (delta depuis last_import)
            - Éviter les imports inutiles (même finish timestamp)

        Args:
            import_results: Liste des résultats de import_data()

        Returns:
            Contexte pour les phases suivantes (blue/green, etc.)

        Raises:
            AirflowException: Si sauvegarde échoue (critique pour la cohérence)
        """
        # Récupère le finish timestamp depuis les infos de polling
        polling_json = VarMgr.get('_current_run_polling', default='{}')
        try:
            polling_result = json.loads(polling_json)
            finish_timestamp = polling_result.get('finish', '')
        except Exception:
            finish_timestamp = ''

        manager = AMUEMetadataManager()
        manager.update_metadata(import_results, finish_timestamp=finish_timestamp)
        logger.info(f"[METADATA] Métadonnées mises à jour pour {len(import_results)} table(s)")

        # Extrait le schéma cible des résultats (pour switch_views)
        target_schema = None
        if import_results:
            target_schema = import_results[0].get('target_schema')

        return {
            'tables_imported': len(import_results),
            'target_schema': target_schema
        }

    @task(task_id='switch_views')
    def switch_views(metadata_result: Dict) -> Dict:
        """
        Bascule les vues vers le nouveau schéma après un import réussi.

        Cette opération est atomique : toutes les vues sont switchées
        dans une seule transaction.

        Args:
            metadata_result: Résultat de save_metadata() avec target_schema

        Returns:
            Résultat du switch :
            {
                "switched": True/False,
                "target_schema": "splus_green",
                "error": None | "message"
            }
        """
        target_schema = metadata_result.get('target_schema')

        if not target_schema:
            logger.info("[SWITCH] Pas de schéma cible - blue/green désactivé")
            return {"switched": False, "reason": "bluegreen disabled"}

        manager = BlueGreenManager()
        if not manager.is_enabled():
            logger.info("[SWITCH] Blue/green désactivé dans la config")
            return {"switched": False, "reason": "bluegreen disabled in config"}

        switcher = ViewSwitcher()
        success = switcher.switch_views_to_schema(target_schema)

        if success:
            # Met à jour l'état blue/green
            manager.mark_import_completed()
            manager.mark_switch_completed()
            logger.info(f"[SWITCH] Vues basculées vers {target_schema}")
            return {
                "switched": True,
                "target_schema": target_schema,
                "error": None
            }
        else:
            logger.error(f"[SWITCH] Échec du switch vers {target_schema}")
            return {
                "switched": False,
                "target_schema": target_schema,
                "error": "Switch failed"
            }

    @task(task_id='send_report')
    def send_report(import_results: List[Dict], switch_result: Dict) -> Dict:
        """
        Génère et envoie le rapport d'exécution par email

        Le rapport contient :
            - Résumé global (tables importées, durée totale)
            - Détail par table (lignes importées, durée)
            - Infos de polling (temps d'attente de l'API)
            - Erreurs éventuelles

        Destinataires configurés via la variable 'amue_report_recipients'.

        Args:
            import_results: Liste des résultats de import_data()
            switch_result: Résultat de switch_views()

        Returns:
            Statut de l'envoi : {"sent": True/False, "recipients": [...]}
        """
        # Récupère les infos de polling stockées en début de DAG
        polling_json = VarMgr.get('_current_run_polling', default='{}')
        try:
            polling_result = json.loads(polling_json)
        except Exception:
            polling_result = {}

        generator = AMUEReportGenerator()
        return generator.generate_and_send(import_results, polling_result)

    # ==========================================================================
    # DÉFINITION DU WORKFLOW (enchaînement des tasks)
    # ==========================================================================

    # Phase 0 : Blue/Green Initialisation
    bluegreen_ctx = init_bluegreen()

    # Phase 1 : Initialisation
    # Retourne la liste des tables à traiter (avec schéma cible injecté)
    tables = wait_for_api_and_select(bluegreen_ctx)

    # Phase 2 : Vérification
    # .expand() crée une task par table (parallélisation automatique)
    verifications = verify_table.expand(table_info=tables)
    validated = validate_tables(verifications)

    # Phase 3 : Import
    # Chaque table est préparée puis importée en parallèle
    prepared = prepare_table.expand(verified_table=validated)
    imported = import_data.expand(prepared_table=prepared)

    # Phase 4 : Finalisation
    # Les métadonnées sont sauvegardées AVANT le switch des vues
    metadata = save_metadata(imported)

    # Phase 5 : Blue/Green Switch
    switch_result = switch_views(metadata)

    # Phase 6 : Rapport
    report = send_report(imported, switch_result)


# ==============================================================================
# INSTANCIATION DU DAG
# ==============================================================================
# Cette ligne crée l'objet DAG qui sera détecté par Airflow
amue_import_dag = amue_multi_table_import()
