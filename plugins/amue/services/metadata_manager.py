"""
Gestionnaire des métadonnées d'import AMUE
Responsable de la persistance des empreintes, dates et statuts
"""
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional

from airflow.exceptions import AirflowException
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Métadonnées d'une table importée"""
    name: str
    finger_print: str
    last_import: str
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
        self.tables_var_name = 'amue_tables_to_import'
        self.last_success_var_name = 'amue_last_successful_run'

    def update_metadata(self, import_results: List[Dict]) -> None:
        """
        Met à jour les métadonnées après des imports réussis

        IMPORTANT: Cette méthode fait échouer le DAG si les métadonnées
        ne peuvent pas être sauvegardées, car cela compromettrait
        les imports différentiels suivants.

        Args:
            import_results: Liste des résultats d'import

        Raises:
            AirflowException: Si mise à jour échoue après tous les retries
        """
        logger.info("Début mise à jour des métadonnées")
        logger.info(f"{len(import_results)} résultats à traiter")

        if not import_results:
            logger.info("Aucun résultat à traiter")
            return

        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # Charge la configuration actuelle
                tables_config = self._load_tables_config()

                # Met à jour chaque table
                updated_count = 0
                for result in import_results:
                    if self._should_update_metadata(result):
                        if self._update_table_metadata(tables_config, result):
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

            except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
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
        Charge la configuration des tables depuis les variables Airflow

        Returns:
            Liste des configurations de tables

        Raises:
            AirflowException: Si chargement échoue
        """
        try:
            tables_var = VarMgr.get(self.tables_var_name)

            # Parse si c'est une chaîne JSON
            if isinstance(tables_var, str):
                tables_config = json.loads(tables_var)
            else:
                tables_config = tables_var

            # Valide que c'est bien une liste
            if not isinstance(tables_config, list):
                raise ValueError("Configuration doit être une liste")

            logger.info(f"{len(tables_config)} tables chargées")
            return tables_config

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            error_msg = f"Impossible de charger la configuration des tables: {str(e)}"
            logger.error(f"[{type(e).__name__}] {error_msg}")
            raise AirflowException(error_msg) from e

    def _update_table_metadata(self, tables_config: List[Dict], result: Dict) -> bool:
        """
        Met à jour les métadonnées d'une table spécifique

        Args:
            tables_config: Configuration complète des tables
            result: Résultat d'import pour cette table

        Returns:
            True si table trouvée et mise à jour
        """
        table_name = result['table_name'].upper()

        # Recherche la table dans la configuration
        for table in tables_config:
            if not isinstance(table, dict):
                continue

            config_name = table.get('name', '').upper()

            if config_name == table_name:
                # Mise à jour des métadonnées
                old_fingerprint = table.get('finger_print', 'none')
                new_fingerprint = result.get('finger_print', '')

                table['finger_print'] = new_fingerprint
                table['last_import'] = datetime.now().isoformat()

                # Mise à jour des clés primaires si récupérées
                if result.get('primary_keys'):
                    old_pk = table.get('primary_key', 'none')
                    new_pk = result['primary_keys']

                    if old_pk != new_pk:
                        logger.info(f"{table_name}: Mise à jour clés primaires")
                        logger.info(f"  Ancien: {old_pk}")
                        logger.info(f"  Nouveau: {new_pk}")
                        table['primary_key'] = new_pk

                fp_old_short = old_fingerprint[:8] if old_fingerprint else 'none'
                fp_new_short = new_fingerprint[:8] if new_fingerprint else 'none'
                logger.info(f"{table_name}:")
                logger.info(f"  - Fingerprint: {fp_old_short}... -> {fp_new_short}...")
                logger.info(f"  - Last import: {table['last_import']}")

                return True

        logger.warning(f"Table {table_name} non trouvée dans la configuration")
        return False

    def _save_tables_config(self, tables_config: List[Dict]) -> None:
        """
        Sauvegarde la configuration des tables

        Args:
            tables_config: Configuration à sauvegarder

        Raises:
            AirflowException: Si sauvegarde échoue
        """
        success = VarMgr.set(self.tables_var_name, json.dumps(tables_config))
        if not success:
            raise AirflowException("Échec de la sauvegarde de la configuration")
        logger.info("Configuration sauvegardée")

    def _save_last_success(self) -> None:
        """
        Enregistre la date du dernier succès global

        Cette date est utilisée pour déterminer l'historique à vérifier
        lors de la prochaine exécution.
        """
        success_date = datetime.now().isoformat()

        success = VarMgr.set(self.last_success_var_name, success_date)
        if success:
            logger.info(f"Dernier succès: {success_date}")
        else:
            logger.warning("Impossible de sauvegarder la date du dernier succès")

    def get_last_success_date(self) -> Optional[datetime]:
        """
        Récupère la date du dernier succès

        Returns:
            Date du dernier succès ou None si jamais exécuté
        """
        try:
            last_success_str = VarMgr.get(self.last_success_var_name, default='')

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
        try:
            tables_config = self._load_tables_config()
            table_name_upper = table_name.upper()

            for table in tables_config:
                if table.get('name', '').upper() == table_name_upper:
                    return TableMetadata(
                        name=table.get('name', ''),
                        finger_print=table.get('finger_print', ''),
                        last_import=table.get('last_import', ''),
                        primary_key=table.get('primary_key', ''),
                        delta=table.get('delta', '')
                    )

        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
            logger.warning(f"[{type(e).__name__}] Erreur récupération métadonnées {table_name}: {str(e)}")

        return None

    def reset_table_metadata(self, table_name: str) -> bool:
        """
        Réinitialise les métadonnées d'une table

        Utile en cas de changement de structure ou de réimport complet

        Args:
            table_name: Nom de la table à réinitialiser

        Returns:
            True si réinitialisation réussie
        """
        try:
            tables_config = self._load_tables_config()
            table_name_upper = table_name.upper()

            for table in tables_config:
                if table.get('name', '').upper() == table_name_upper:
                    table['finger_print'] = ''
                    table['last_import'] = ''

                    self._save_tables_config(tables_config)
                    logger.info(f"Table {table_name} réinitialisée")
                    return True

            logger.warning(f"Table {table_name} non trouvée")
            return False

        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
            logger.error(f"[{type(e).__name__}] Échec réinitialisation {table_name}: {str(e)}")
            return False
