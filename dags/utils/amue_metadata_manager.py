"""
Gestionnaire des métadonnées d'import
"""
import json
from datetime import datetime
from typing import List, Dict
from airflow.sdk import Variable


class AMUEMetadataManager:
    """Gère les métadonnées d'import (fingerprints, dates, etc.)"""

    def update_metadata(self, import_results: List[Dict]) -> None:
        """Met à jour les métadonnées après un import réussi"""
        print("[METADATA] Mise à jour")

        try:
            # Charge la configuration actuelle
            tables_info = self._load_tables_config()

            # Met à jour chaque table
            for result in import_results:
                if result.get('status') == 'success':
                    self._update_table_metadata(tables_info, result)

            # Sauvegarde la configuration
            self._save_tables_config(tables_info)

            # Enregistre la date du dernier succès
            self._save_last_success()

            print("[METADATA] Succès enregistré")

        except Exception as e:
            print(f"[WARN] MAJ metadata: {e}")

    def _load_tables_config(self) -> List[Dict]:
        """Charge la configuration des tables"""
        tables_var = Variable.get('amue_tables_to_import')
        return json.loads(tables_var) if isinstance(tables_var, str) else tables_var

    def _update_table_metadata(self, tables_info: List[Dict], result: Dict) -> None:
        """Met à jour les métadonnées d'une table"""
        table_name = result['table_name']

        for table in tables_info:
            if table.get('name', '').lower() == table_name.lower():
                table['finger_print'] = result.get('finger_print', '')
                table['last_import'] = datetime.now().isoformat()
                print(f"[METADATA] {table_name}: MAJ")
                break

    def _save_tables_config(self, tables_info: List[Dict]) -> None:
        """Sauvegarde la configuration des tables"""
        try:
            from airflow.sdk.definitions.variable import Variable as SdkVariable
            SdkVariable.set('amue_tables_to_import', json.dumps(tables_info))
        except:
            Variable.set('amue_tables_to_import', json.dumps(tables_info))

    def _save_last_success(self) -> None:
        """Enregistre la date du dernier succès"""
        try:
            from airflow.sdk.definitions.variable import Variable as SdkVariable
            SdkVariable.set('amue_last_successful_run', datetime.now().isoformat())
        except:
            Variable.set('amue_last_successful_run', datetime.now().isoformat())