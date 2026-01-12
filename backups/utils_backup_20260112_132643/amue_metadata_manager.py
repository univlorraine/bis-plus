"""
Gestionnaire des métadonnées d'import AMUE
Responsable de la persistance des empreintes, dates et statuts
"""
import json
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from airflow.sdk import Variable
from airflow.exceptions import AirflowException


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
    - Gestion thread-safe des variables Airflow
    """

    def __init__(self):
        """Initialise le gestionnaire de métadonnées"""
        self.tables_var_name = 'amue_tables_to_import'
        self.last_success_var_name = 'amue_last_successful_run'

    def update_metadata(self, import_results: List[Dict]) -> None:
        """
        Met à jour les métadonnées après des imports réussis

        Args:
            import_results: Liste des résultats d'import

        Raises:
            AirflowException: Si mise à jour échoue
        """
        print("[METADATA] Début mise à jour des métadonnées")
        print(f"[METADATA] {len(import_results)} résultats à traiter")

        if not import_results:
            print("[METADATA] Aucun résultat à traiter")
            return

        try:
            # Charge la configuration actuelle
            tables_config = self._load_tables_config()
            original_count = len(tables_config)

            # Met à jour chaque table
            updated_count = 0
            for result in import_results:
                if self._should_update_metadata(result):
                    if self._update_table_metadata(tables_config, result):
                        updated_count += 1

            # Sauvegarde la configuration
            if updated_count > 0:
                self._save_tables_config(tables_config)
                print(f"[METADATA] {updated_count}/{len(import_results)} tables mises à jour")
            else:
                print("[METADATA] Aucune mise à jour nécessaire")

            # Enregistre la date du dernier succès global
            self._save_last_success()

            print("[METADATA] Mise à jour terminée avec succès")

        except Exception as e:
            error_msg = f"Erreur lors de la mise à jour des métadonnées: {str(e)}"
            print(f"[ERROR] {error_msg}")
            # On log mais on ne fait pas échouer le DAG
            print("[WARN] Métadonnées non mises à jour mais import réussi")

    def _should_update_metadata(self, result: Dict) -> bool:
        """
        Détermine si on doit mettre à jour les métadonnées pour ce résultat

        Args:
            result: Résultat d'import d'une table

        Returns:
            True si mise à jour nécessaire
        """
        if result.get('status') != 'success':
            print(f"[METADATA] Skip {result.get('table_name', 'unknown')}: statut {result.get('status')}")
            return False

        if not result.get('table_name'):
            print("[METADATA] Skip: pas de nom de table")
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
            tables_var = Variable.get(self.tables_var_name)

            # Parse si c'est une chaîne JSON
            if isinstance(tables_var, str):
                tables_config = json.loads(tables_var)
            else:
                tables_config = tables_var

            # Valide que c'est bien une liste
            if not isinstance(tables_config, list):
                raise ValueError("Configuration doit être une liste")

            print(f"[METADATA] {len(tables_config)} tables chargées")
            return tables_config

        except Exception as e:
            error_msg = f"Impossible de charger la configuration des tables: {str(e)}"
            print(f"[ERROR] {error_msg}")
            raise AirflowException(error_msg) from e

    def _update_table_metadata(self, tables_config: List[Dict], result: Dict) -> bool:
        """
        Met à jour les métadonnées d'une table spécifique

        NOUVEAU: Met à jour aussi les clés primaires si elles ont été récupérées

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

                # NOUVEAU: Mise à jour des clés primaires si récupérées
                if result.get('primary_keys'):
                    old_pk = table.get('primary_key', 'none')
                    new_pk = result['primary_keys']

                    if old_pk != new_pk:
                        print(f"[METADATA] {table_name}: Mise à jour clés primaires")
                        print(f"  Ancien: {old_pk}")
                        print(f"  Nouveau: {new_pk}")
                        table['primary_key'] = new_pk

                print(f"[METADATA] {table_name}:")
                print(f"  - Fingerprint: {old_fingerprint[:8]}... → {new_fingerprint[:8]}...")
                print(f"  - Last import: {table['last_import']}")

                return True

        print(f"[WARN] Table {table_name} non trouvée dans la configuration")
        return False

    def _save_tables_config(self, tables_config: List[Dict]) -> None:
        """
        Sauvegarde la configuration des tables

        Args:
            tables_config: Configuration à sauvegarder

        Raises:
            AirflowException: Si sauvegarde échoue
        """
        try:
            # Tente d'abord avec le SDK (Airflow 3.x)
            try:
                from airflow.sdk.definitions.variable import Variable as SdkVariable
                SdkVariable.set(self.tables_var_name, json.dumps(tables_config))
                print("[METADATA] Configuration sauvegardée (SDK)")
                return
            except ImportError:
                pass

            # Fallback sur l'API classique
            Variable.set(self.tables_var_name, json.dumps(tables_config))
            print("[METADATA] Configuration sauvegardée (API)")

        except Exception as e:
            error_msg = f"Échec sauvegarde configuration: {str(e)}"
            print(f"[ERROR] {error_msg}")
            raise AirflowException(error_msg) from e

    def _save_last_success(self) -> None:
        """
        Enregistre la date du dernier succès global

        Cette date est utilisée pour déterminer l'historique à vérifier
        lors de la prochaine exécution.
        """
        success_date = datetime.now().isoformat()

        try:
            # Tente avec le SDK
            try:
                from airflow.sdk.definitions.variable import Variable as SdkVariable
                SdkVariable.set(self.last_success_var_name, success_date)
                print(f"[METADATA] Dernier succès: {success_date} (SDK)")
                return
            except ImportError:
                pass

            # Fallback
            Variable.set(self.last_success_var_name, success_date)
            print(f"[METADATA] Dernier succès: {success_date} (API)")

        except Exception as e:
            print(f"[WARN] Échec sauvegarde dernier succès: {str(e)}")
            # On ne fait pas échouer le DAG pour ça

    def get_last_success_date(self) -> Optional[datetime]:
        """
        Récupère la date du dernier succès

        Returns:
            Date du dernier succès ou None si jamais exécuté
        """
        try:
            last_success_str = Variable.get(self.last_success_var_name, default='')

            if last_success_str:
                return datetime.fromisoformat(last_success_str)

        except Exception as e:
            print(f"[WARN] Impossible de récupérer dernier succès: {str(e)}")

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

        except Exception as e:
            print(f"[WARN] Erreur récupération métadonnées {table_name}: {str(e)}")

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
                    print(f"[METADATA] Table {table_name} réinitialisée")
                    return True

            print(f"[WARN] Table {table_name} non trouvée")
            return False

        except Exception as e:
            print(f"[ERROR] Échec réinitialisation {table_name}: {str(e)}")
            return False