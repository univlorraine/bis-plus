"""
Filtrage et sélection des tables AMUE à importer
"""
import json
from typing import Dict, List
from airflow.sdk import Variable


class AMUETableFilter:
    """Filtre les tables à traiter selon leur statut et historique"""

    def __init__(self, tables_config: List[Dict] = None):
        if tables_config is None:
            tables_config = self._load_config()
        self.tables_config = tables_config

    def filter_tables(self, current_status: Dict, history: Dict) -> List[Dict]:
        """Filtre les tables selon statut actuel et historique"""
        print(f"[FILTER] Filtrage de {len(self.tables_config)} tables configurées")

        tables_to_process = []

        for table_config in self.tables_config:
            if not isinstance(table_config, dict) or 'name' not in table_config:
                continue

            table_name = table_config['name'].upper()

            if table_name not in current_status:
                print(f"[FILTER] {table_name}: Non trouvée dans le statut actuel")
                continue

            # Enrichit la config
            enriched_config = self._enrich_table_config(
                table_config,
                current_status[table_name],
                history
            )

            # Détermine si on traite cette table
            if self._should_process_table(enriched_config):
                tables_to_process.append(enriched_config)
                print(f"[FILTER] {table_name}: À traiter ({enriched_config['import_type']})")
            else:
                print(f"[FILTER] {table_name}: Skip")

        print(f"[FILTER] {len(tables_to_process)} tables à traiter")
        return tables_to_process

    def _load_config(self) -> List[Dict]:
        """Charge la configuration des tables depuis les variables"""
        default_config = json.dumps([{
            "name": "CSKS",
            "primary_key": "",
            "delta": "",
            "last_import": "",
            "finger_print": ""
        }])

        tables_var = Variable.get('amue_tables_to_import', default=default_config)
        tables_config = json.loads(tables_var) if isinstance(tables_var, str) else tables_var

        return tables_config if isinstance(tables_config, list) else []

    def _enrich_table_config(self, table_config: Dict, current_status: Dict, history: Dict) -> Dict:
        """Enrichit la config d'une table avec statut et historique"""
        enriched = table_config.copy()

        # Ajoute les valeurs par défaut
        enriched.setdefault('primary_key', '')
        enriched.setdefault('delta', '')
        enriched.setdefault('last_import', '')
        enriched.setdefault('finger_print', '')

        # Ajoute le statut actuel
        enriched['current_status'] = current_status

        # Vérifie l'historique
        history_ok, last_ok_date = self._check_history(
            enriched['name'].upper(),
            history
        )

        enriched['history_ok'] = history_ok
        enriched['last_ok_date'] = last_ok_date

        return enriched

    def _check_history(self, table_name: str, history: Dict) -> tuple:
        """Vérifie l'historique d'une table"""
        status_by_date = history.get('status_by_date', {})

        for date_str in sorted(status_by_date.keys(), reverse=True):
            date_info = status_by_date[date_str]
            tables_in_date = date_info.get('tables_status', {})

            if table_name in tables_in_date:
                table_status = tables_in_date[table_name]
                if table_status['status'] != 'OK':
                    return False, None
                return True, date_str

        return True, None

    def _should_process_table(self, table_config: Dict) -> bool:
        """Détermine si une table doit être traitée"""
        current_status = table_config['current_status']['status']
        history_ok = table_config.get('history_ok', False)

        if current_status != 'OK' or not history_ok:
            return False

        # Détermine le type d'import
        has_last_import = bool(table_config.get('last_import'))
        has_delta = bool(table_config.get('delta'))

        table_config['import_type'] = 'differential' if (has_last_import and has_delta) else 'full'
        table_config['to_process'] = True

        return True