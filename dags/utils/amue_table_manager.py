"""
Gestionnaire de création et mise à jour des tables
"""
from typing import Dict, List
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


class AMUETableManager:
    """Gère la création et la structure des tables PostgreSQL"""

    def __init__(self, postgres_hook: PostgresHook = None):
        self.postgres_hook = postgres_hook or PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )
        self.environment = Variable.get('environment', default='production')

    def manage_table(self, structure_info: Dict) -> Dict:
        """Gère la structure d'une table (création si nécessaire)"""
        table_name = structure_info['table_name']
        exists = structure_info['exists']

        print(f"[TABLE_MGT] Table: {table_name}, exists: {exists}, env: {self.environment}")

        # En production, on ne crée jamais de tables
        if self.environment == 'production' or exists:
            return self._existing_table_info(structure_info)

        # Création en dev uniquement
        print(f"[DEV] Création table {table_name}")
        return self._create_table(structure_info)

    def _existing_table_info(self, structure_info: Dict) -> Dict:
        """Retourne les infos d'une table existante"""
        return {
            'table_name': structure_info['table_name'].lower(),
            'columns': [col['name'].lower() for col in structure_info['columns']],
            'primary_keys': structure_info['primary_keys'],
            'created': False,
            'status': 'success'
        }

    def _create_table(self, structure_info: Dict) -> Dict:
        """Crée une nouvelle table"""
        table_name = structure_info['table_name']
        table_lower = table_name.lower()
        columns = structure_info['columns']
        primary_keys = [pk.strip() for pk in structure_info['primary_keys'].split(',') if pk.strip()]

        try:
            # Construit la définition des colonnes
            column_definitions = []
            for col in columns:
                col_name = col['name'].lower()
                col_type = col['type_postgres']
                column_definitions.append(f"{col_name} {col_type}")

            # Ajoute la contrainte de clé primaire si présente
            pk_constraint = ''
            if primary_keys:
                pk_cols = ', '.join([pk.lower() for pk in primary_keys])
                pk_constraint = f", PRIMARY KEY ({pk_cols})"

            # Crée la table
            create_sql = f"""
                DROP TABLE IF EXISTS {table_lower} CASCADE;
                CREATE TABLE {table_lower} (
                    {', '.join(column_definitions)}
                    {pk_constraint}
                );
            """

            self.postgres_hook.run(create_sql)
            print(f"[DEV] Table {table_lower} créée")

            return {
                'table_name': table_lower,
                'columns': [col['name'].lower() for col in columns],
                'primary_keys': structure_info['primary_keys'],
                'created': True,
                'status': 'success'
            }

        except Exception as e:
            error_msg = f"Erreur création {table_name}: {e}"
            print(f"[ERROR] {error_msg}")
            raise AirflowException(error_msg)