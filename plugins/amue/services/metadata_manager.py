"""
Gestionnaire des métadonnées d'import AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module persiste les métadonnées d'import dans les variables Airflow.
Ces métadonnées sont ESSENTIELLES pour :
    - Détecter les changements de structure (fingerprint)
    - Permettre l'import différentiel (last_report_start global dans amue_state)
    - Tracer l'historique des imports

================================================================================
MÉTADONNÉES GÉRÉES
================================================================================

Pour chaque table importée, le gestionnaire sauvegarde :

┌─────────────────┬────────────────────────────────────────────────────────────┐
│ Métadonnée      │ Description                                                │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ fingerprint_API │ Hash MD5 structure originale API + PKs API                  │
│                 │ → Détecte les changements côté fournisseur                 │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ fingerprint_UL  │ Hash MD5 structure transformée PG + PKs config             │
│                 │ → Détecte les changements côté local                       │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ primary_key     │ Liste des colonnes formant la clé primaire (CSV)           │
│                 │ → Utilisé pour construire les UPSERT                       │
└─────────────────┴────────────────────────────────────────────────────────────┘

NOTE : La référence temporelle pour l'import différentiel (last_report_start) est
stockée globalement dans splus_admin.amue_state, pas par table.

================================================================================
STOCKAGE
================================================================================

Les métadonnées sont stockées dans la table PostgreSQL splus_admin.amue_tables
via TableConfigManager.

Les timestamps de synchro (last_finish_timestamp, last_successful_run) sont
stockés dans la table PostgreSQL splus_admin.amue_state via AdminStateManager.

================================================================================
GESTION DES ERREURS
================================================================================

La sauvegarde des métadonnées est CRITIQUE. En cas d'échec :
    1. Retry avec backoff exponentiel (3 tentatives)
    2. Si échec persistant : le DAG échoue

Pourquoi ? Si les métadonnées ne sont pas sauvegardées :
    - Le prochain import ne détectera pas les changements de structure
    - L'import différentiel réimportera toutes les données

================================================================================
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional

from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Métadonnées d'une table importée"""
    name: str
    fingerprint_API: str
    fingerprint_UL: str
    primary_key: str = ''
    delta: str = ''


class AMUEMetadataManager:
    """
    Gestionnaire des métadonnées d'import

    Responsabilités :
    - Mise à jour des fingerprints après import
    - Enregistrement des dates de dernier import
    - Sauvegarde de la date du dernier succès global
    - Gestion avec retry pour éviter les pertes de données
    """

    # Configuration retry
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 2

    def __init__(self):
        """Initialise le gestionnaire de métadonnées"""

    def update_metadata(self, import_results: List[Dict], finish_timestamp: str = None, report_start: str = None) -> None:
        """
        Met à jour les métadonnées après des imports réussis

        IMPORTANT: Cette méthode fait échouer le DAG si les métadonnées
        ne peuvent pas être sauvegardées, car cela compromettrait
        les imports différentiels suivants.

        Args:
            import_results: Liste des résultats d'import
            finish_timestamp: Timestamp finish de l'API (pour le polling)
            report_start: Date start du rapport API AMUE (référence globale pour le mode différentiel)

        Raises:
            AirflowException: Si mise à jour échoue après tous les retries
        """
        logger.info("Début mise à jour des métadonnées")
        logger.info(f"{len(import_results)} résultats à traiter")

        # Stocke le report_start pour l'utiliser dans _update_table_metadata
        self._report_start = report_start or ''

        # Sauvegarde le finish timestamp pour le prochain polling
        if finish_timestamp:
            self._save_finish_timestamp(finish_timestamp)

        # Sauvegarde le report_start comme référence globale pour les imports différentiels
        if report_start:
            self._save_report_start(report_start)

        if not import_results:
            logger.info("Aucun résultat à traiter")
            return

        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # Charge la configuration actuelle
                tables_config = self._load_tables_config()
                tables_index = {
                    t.get('name', '').upper(): t
                    for t in tables_config
                    if isinstance(t, dict)
                }

                # Met à jour chaque table
                updated_count = 0
                for result in import_results:
                    logger.debug("[METADATA] Traitement: %s", result)

                    if self._should_update_metadata(result):
                        if self._update_table_metadata(tables_index, result):
                            updated_count += 1

                # Sauvegarde la configuration
                if updated_count > 0:
                    self._save_tables_config(tables_config)
                    logger.info(f"{updated_count}/{len(import_results)} tables mises à jour")
                else:
                    logger.info("Aucune mise à jour nécessaire")

                # Enregistre la date du dernier succès global
                self._save_last_success()

                logger.info("Mise à jour terminée avec succès")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"[{type(e).__name__}] Tentative {attempt + 1}/{self.MAX_RETRIES} échouée: {e}")

                if attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_DELAY_SECONDS * (2 ** attempt)  # Backoff exponentiel
                    logger.info(f"Retry dans {wait_time}s...")
                    time.sleep(wait_time)

        # Échec après tous les retries - FAIL EXPLICITE
        error_msg = f"Impossible de sauvegarder les métadonnées après {self.MAX_RETRIES} tentatives: {last_error}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

    def _should_update_metadata(self, result: Dict) -> bool:
        """
        Détermine si on doit mettre à jour les métadonnées pour ce résultat

        Args:
            result: Résultat d'import d'une table

        Returns:
            True si mise à jour nécessaire
        """
        if result.get('status') != 'success':
            logger.info(f"Skip {result.get('table_name', 'unknown')}: statut {result.get('status')}")
            return False

        if not result.get('table_name'):
            logger.info("Skip: pas de nom de table")
            return False

        return True

    def _load_tables_config(self) -> List[Dict]:
        """
        Charge la configuration des tables depuis splus_admin.amue_tables.

        Returns:
            Liste des configurations de tables

        Raises:
            Exception: Si chargement échoue (pour déclencher le retry du caller)
        """
        from amue.services.table_config_manager import TableConfigManager
        return TableConfigManager().get_tables_config()

    def _update_table_metadata(self, tables_index: Dict[str, Dict], result: Dict) -> bool:
        """
        Met à jour les métadonnées d'une table spécifique

        Args:
            tables_index: Index {NOM_UPPER: table_dict} pré-calculé depuis la configuration
            result: Résultat d'import pour cette table

        Returns:
            True si table trouvée et mise à jour
        """
        table_name = result['table_name'].upper()

        table = tables_index.get(table_name)
        if table is None:
            logger.warning(f"Table {table_name} non trouvée dans la configuration")
            return False

        # Mise à jour des métadonnées
        old_fp_api = table.get('fingerprint_API', 'none')
        old_fp_ul = table.get('fingerprint_UL', 'none')
        new_fp_api = result.get('fingerprint_API', '')
        new_fp_ul = result.get('fingerprint_UL', '')

        table['fingerprint_API'] = new_fp_api
        table['fingerprint_UL'] = new_fp_ul
        # Supprimer l'ancien champ s'il existe
        table.pop('finger_print', None)

        # Mise à jour des clés primaires uniquement si pas déjà définies
        if result.get('primary_keys') and not table.get('primary_key'):
            table['primary_key'] = result['primary_keys']
            logger.info(f"{table_name}: Clés primaires initialisées: {result['primary_keys']}")

        logger.info(f"{table_name}:")
        logger.info(f"  - fingerprint_API: {old_fp_api[:8] if old_fp_api else 'none'}... -> {new_fp_api[:8] if new_fp_api else 'none'}...")
        logger.info(f"  - fingerprint_UL: {old_fp_ul[:8] if old_fp_ul else 'none'}... -> {new_fp_ul[:8] if new_fp_ul else 'none'}...")

        return True

    def _save_tables_config(self, tables_config: List[Dict]) -> None:
        """
        Sauvegarde la configuration des tables dans splus_admin.amue_tables.

        Args:
            tables_config: Configuration à sauvegarder

        Raises:
            Exception: Si sauvegarde échoue (pour déclencher le retry du caller)
        """
        from amue.services.table_config_manager import TableConfigManager
        TableConfigManager().save_tables_config(tables_config)
        logger.info("Configuration sauvegardée")

    def _save_last_success(self) -> None:
        """
        Enregistre la date du dernier succès global dans la BDD.

        Cette date est utilisée pour déterminer l'historique à vérifier
        lors de la prochaine exécution.
        """
        from amue.services.admin_state_manager import AdminStateManager
        success_date = datetime.now().isoformat()
        AdminStateManager().set_last_successful_run(success_date)
        logger.info(f"Dernier succès: {success_date}")

    def _save_report_start(self, report_start: str) -> None:
        """
        Sauvegarde le timestamp de début du rapport AMUE dans la BDD.

        Ce timestamp est utilisé pour les imports différentiels : toutes les
        tables delta filtrent leurs données avec delta_column >= last_report_start.

        Args:
            report_start: Valeur ISO 8601 du champ 'start' retourné par l'API AMUE
        """
        from amue.services.admin_state_manager import AdminStateManager
        AdminStateManager().set_last_report_start(report_start)
        logger.info(f"Report start enregistré: {report_start}")

    def _save_finish_timestamp(self, finish_timestamp: str) -> None:
        """
        Sauvegarde le timestamp finish de l'API dans la BDD.

        Ce timestamp est utilisé par le polling pour détecter si de nouvelles
        données sont disponibles. Si le timestamp est identique au précédent,
        l'import est ignoré.

        Args:
            finish_timestamp: Valeur du timestamp finish retourné par l'API
        """
        from amue.services.admin_state_manager import AdminStateManager
        mgr = AdminStateManager()
        old_timestamp = mgr.get_last_finish_timestamp()
        mgr.set_last_finish_timestamp(finish_timestamp)
        if old_timestamp:
            logger.info(f"Finish timestamp mis à jour: {old_timestamp} -> {finish_timestamp}")
        else:
            logger.info(f"Finish timestamp enregistré: {finish_timestamp}")

    def get_last_success_date(self) -> Optional[datetime]:
        """
        Récupère la date du dernier succès depuis la BDD.

        Returns:
            Date du dernier succès ou None si jamais exécuté
        """
        from amue.services.admin_state_manager import AdminStateManager
        try:
            last_success_str = AdminStateManager().get_last_successful_run()
            if last_success_str:
                return datetime.fromisoformat(last_success_str)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"[{type(e).__name__}] Impossible de récupérer dernier succès: {str(e)}")
        return None

    def get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        """
        Récupère les métadonnées d'une table spécifique

        Args:
            table_name: Nom de la table

        Returns:
            Métadonnées de la table ou None si non trouvée
        """
        from amue.services.table_config_manager import TableConfigManager
        try:
            table = TableConfigManager().get_table_metadata(table_name)
            if table is None:
                return None
            return TableMetadata(
                name=table.get('name', ''),
                fingerprint_API=table.get('fingerprint_API', ''),
                fingerprint_UL=table.get('fingerprint_UL', ''),
                primary_key=table.get('primary_key', ''),
                delta=table.get('delta', '')
            )
        except Exception as e:
            logger.warning(f"[{type(e).__name__}] Erreur récupération métadonnées {table_name}: {str(e)}")
            return None

    def reset_table_metadata(self, table_name: str) -> bool:
        """
        Réinitialise les métadonnées d'une table

        Utile en cas de changement de structure ou de réimport complet.

        Args:
            table_name: Nom de la table à réinitialiser

        Returns:
            True si réinitialisation réussie
        """
        from amue.services.table_config_manager import TableConfigManager
        return TableConfigManager().reset_table_metadata(table_name)
