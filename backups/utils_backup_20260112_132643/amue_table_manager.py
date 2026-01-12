"""
Gestionnaire de création et mise à jour des tables PostgreSQL
Responsable de la gestion du schéma DDL des tables AMUE
"""
from typing import Dict, List, Optional
from dataclasses import dataclass
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


@dataclass
class TableManagementResult:
    """Résultat d'une opération de gestion de table"""
    table_name: str
    columns: List[str]
    primary_keys: str
    created: bool
    status: str
    error: Optional[str] = None


class AMUETableManager:
    """
    Gère la création et la structure des tables PostgreSQL.

    Règles de gestion :
    - En production : Aucune création de table (lecture seule)
    - En développement : Création automatique si table absente
    - Validation systématique de la structure avant opération
    """

    def __init__(self, postgres_hook: PostgresHook = None):
        """
        Initialise le gestionnaire de tables

        Args:
            postgres_hook: Hook PostgreSQL personnalisé (optionnel)
        """
        self.postgres_hook = postgres_hook or self._create_default_hook()
        self.environment = Variable.get('environment', default='production')

    def _create_default_hook(self) -> PostgresHook:
        """Crée le hook PostgreSQL par défaut"""
        return PostgresHook(
            postgres_conn_id='postgres_data',
            options='-c search_path=splus'
        )

    def manage_table(self, structure_info: Dict) -> Dict:
        """
        Point d'entrée principal pour la gestion d'une table

        Args:
            structure_info: Informations de structure de la table

        Returns:
            Dictionnaire avec résultat de l'opération

        Raises:
            AirflowException: Si opération échoue en production
        """
        table_name = structure_info['table_name']
        exists = structure_info['exists']

        print(f"[TABLE_MGT] Table: {table_name}")
        print(f"[TABLE_MGT] Environment: {self.environment}")
        print(f"[TABLE_MGT] Exists: {exists}")

        # Validation de la structure
        self._validate_structure_info(structure_info)

        # Décision selon l'environnement
        if self.environment == 'production':
            return self._handle_production_table(structure_info, exists)

        return self._handle_dev_table(structure_info, exists)

    def _validate_structure_info(self, structure_info: Dict) -> None:
        """
        Valide la complétude des informations de structure

        Raises:
            AirflowException: Si structure invalide
        """
        required_fields = ['table_name', 'columns', 'primary_keys', 'exists']
        missing = [f for f in required_fields if f not in structure_info]

        if missing:
            raise AirflowException(
                f"Structure invalide pour table. Champs manquants: {missing}"
            )

        if not structure_info['columns']:
            raise AirflowException(
                f"Table {structure_info['table_name']}: aucune colonne définie"
            )

    def _handle_production_table(self, structure_info: Dict, exists: bool) -> Dict:
        """
        Gère une table en environnement de production

        En production, on refuse catégoriquement toute création.
        """
        if not exists:
            table_name = structure_info['table_name']
            raise AirflowException(
                f"[PRODUCTION] Table {table_name} inexistante. "
                "Création interdite en production. Créez la table manuellement."
            )

        print(f"[PRODUCTION] Utilisation table existante")
        return self._build_existing_table_result(structure_info)

    def _handle_dev_table(self, structure_info: Dict, exists: bool) -> Dict:
        """Gère une table en environnement de développement"""
        if exists:
            print(f"[DEV] Utilisation table existante")
            return self._build_existing_table_result(structure_info)

        print(f"[DEV] Création de la table")
        return self._create_table(structure_info)

    def _build_existing_table_result(self, structure_info: Dict) -> Dict:
        """
        Construit le résultat pour une table existante

        Returns:
            Dictionnaire normalisé avec informations de la table
        """
        result = TableManagementResult(
            table_name=structure_info['table_name'].lower(),
            columns=[col['name'].lower() for col in structure_info['columns']],
            primary_keys=structure_info['primary_keys'],
            created=False,
            status='success'
        )

        return self._result_to_dict(result)

    def _create_table(self, structure_info: Dict) -> Dict:
        """
        Crée une nouvelle table dans PostgreSQL

        Args:
            structure_info: Informations complètes de structure

        Returns:
            Résultat de la création

        Raises:
            AirflowException: Si création échoue
        """
        table_name = structure_info['table_name'].lower()
        columns = structure_info['columns']
        primary_keys_str = structure_info['primary_keys']

        try:
            # Génère le DDL
            create_sql = self._build_create_table_sql(
                table_name,
                columns,
                primary_keys_str
            )

            # Exécute la création
            print(f"[DEV] Exécution CREATE TABLE {table_name}")
            self.postgres_hook.run(create_sql)
            print(f"[DEV] Table {table_name} créée avec succès")

            # Construit le résultat
            result = TableManagementResult(
                table_name=table_name,
                columns=[col['name'].lower() for col in columns],
                primary_keys=primary_keys_str,
                created=True,
                status='success'
            )

            return self._result_to_dict(result)

        except Exception as e:
            error_msg = f"Échec création table {table_name}: {str(e)}"
            print(f"[ERROR] {error_msg}")

            result = TableManagementResult(
                table_name=table_name,
                columns=[],
                primary_keys='',
                created=False,
                status='error',
                error=error_msg
            )

            raise AirflowException(error_msg) from e

    def _build_create_table_sql(
        self,
        table_name: str,
        columns: List[Dict],
        primary_keys_str: str
    ) -> str:
        """
        Construit le SQL de création de table

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes avec types
            primary_keys_str: Clés primaires séparées par virgules

        Returns:
            Instruction SQL CREATE TABLE complète
        """
        # Définitions des colonnes
        column_defs = [
            f"{col['name'].lower()} {col['type_postgres']}"
            for col in columns
        ]

        # Contrainte de clé primaire
        pk_constraint = self._build_primary_key_constraint(primary_keys_str)

        # Assembly du SQL
        columns_sql = ',\n    '.join(column_defs)

        create_sql = f"""
            DROP TABLE IF EXISTS {table_name} CASCADE;
            
            CREATE TABLE {table_name} (
                {columns_sql}{pk_constraint}
            );
        """

        return create_sql

    def _build_primary_key_constraint(self, primary_keys_str: str) -> str:
        """
        Construit la clause PRIMARY KEY

        Args:
            primary_keys_str: Clés primaires CSV

        Returns:
            Clause SQL ou chaîne vide si pas de PK
        """
        if not primary_keys_str or not primary_keys_str.strip():
            print("[WARN] Aucune clé primaire définie")
            return ''

        pk_list = [
            pk.strip().lower()
            for pk in primary_keys_str.split(',')
            if pk.strip()
        ]

        if not pk_list:
            return ''

        pk_cols = ', '.join(pk_list)
        print(f"[TABLE_MGT] Clés primaires: {pk_cols}")

        return f",\n    PRIMARY KEY ({pk_cols})"

    def _result_to_dict(self, result: TableManagementResult) -> Dict:
        """Convertit un résultat en dictionnaire"""
        return {
            'table_name': result.table_name,
            'columns': result.columns,
            'primary_keys': result.primary_keys,
            'created': result.created,
            'status': result.status,
            'error': result.error
        }