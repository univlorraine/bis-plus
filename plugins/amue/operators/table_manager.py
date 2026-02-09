"""
Gestionnaire de création et mise à jour des tables PostgreSQL.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module gère le schéma DDL (Data Definition Language) des tables AMUE dans
PostgreSQL. Il est responsable de la création des tables en développement
et de la validation de leur existence en production.

RÈGLES DE GESTION SELON L'ENVIRONNEMENT :

┌─────────────────┬────────────────────┬────────────────────────────────────┐
│ Environnement   │ Table existe       │ Action                             │
├─────────────────┼────────────────────┼────────────────────────────────────┤
│ PRODUCTION      │ Oui                │ Utilisation de la table existante  │
│ PRODUCTION      │ Non                │ ERREUR - Création interdite        │
│ DÉVELOPPEMENT   │ Oui                │ Utilisation de la table existante  │
│ DÉVELOPPEMENT   │ Non                │ Création automatique (DROP IF + CREATE) │
└─────────────────┴────────────────────┴────────────────────────────────────┘

PHILOSOPHIE :
    - En PRODUCTION : lecture seule du schéma (sécurité maximale)
    - En DEV : création automatique pour faciliter le développement
    - La structure est toujours validée avant toute opération

================================================================================
GÉNÉRATION DDL
================================================================================

Le DDL généré inclut :
    - DROP TABLE IF EXISTS ... CASCADE (en dev uniquement)
    - CREATE TABLE avec colonnes typées
    - Contrainte PRIMARY KEY si clés définies

Exemple de DDL généré :
    DROP TABLE IF EXISTS csks CASCADE;
    CREATE TABLE csks (
        bukrs VARCHAR(4),
        kostl VARCHAR(10),
        datab DATE,
        PRIMARY KEY (bukrs, kostl)
    );

================================================================================
CONFIGURATION
================================================================================

Variable Airflow :
    - environment : "dev" ou "production" (défaut: "production")

Connexion PostgreSQL :
    - postgres_conn_id : "postgres_data"
    - schema : "splus"

================================================================================
"""
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import DatabaseError, IntegrityError, ProgrammingError
from amue.exceptions import AMUESchemaError, AMUETableNotFoundError, AMUEDatabaseError
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.hooks import create_postgres_hook
from amue.utils.schema_utils import SchemaQualifier
from amue.services.view_switcher import ViewSwitcher

logger = logging.getLogger(__name__)


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
    - Support blue/green : création dans le schéma cible spécifié
    """

    def __init__(self, postgres_hook: PostgresHook = None, target_schema: Optional[str] = None):
        """
        Initialise le gestionnaire de tables

        Args:
            postgres_hook: Hook PostgreSQL personnalisé (optionnel)
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue')
                          Si None, utilise le schéma par défaut 'splus'
        """
        self._schema_qualifier = SchemaQualifier(target_schema)
        self.postgres_hook = postgres_hook or self._create_default_hook()
        self.environment = VarMgr.get('environment', default='production')
        self.default_source = VarMgr.get('amue_default_source', default='sifac_plus')
        # ViewSwitcher pour créer les vues dans splus (uniquement en mode blue/green)
        self.view_switcher = ViewSwitcher(self.postgres_hook) if target_schema else None

    @property
    def target_schema(self) -> Optional[str]:
        """Retourne le schéma cible."""
        return self._schema_qualifier.target_schema

    @target_schema.setter
    def target_schema(self, value: Optional[str]) -> None:
        """Définit le schéma cible."""
        self._schema_qualifier.target_schema = value

    def _create_default_hook(self) -> PostgresHook:
        """Crée le hook PostgreSQL par défaut via factory."""
        if self._schema_qualifier.target_schema:
            return create_postgres_hook(bluegreen_schema=self._schema_qualifier.target_schema)
        return create_postgres_hook()

    def _ensure_view_exists(self, table_name: str) -> None:
        """
        S'assure que la vue existe dans le schéma splus (mode blue/green uniquement).

        En mode blue/green, chaque table dans splus_blue/splus_green doit avoir
        une vue correspondante dans splus. Cette méthode crée la vue si elle
        n'existe pas.

        Args:
            table_name: Nom de la table
        """
        if not self.view_switcher or not self.target_schema:
            # Pas en mode blue/green, pas de vue à créer
            return

        table_name_lower = table_name.lower()

        # Vérifie si la vue existe déjà
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_schema = 'splus'
                  AND table_name = %s
            )
        """
        result = self.postgres_hook.get_first(query, parameters=(table_name_lower,))
        view_exists = result[0] if result else False

        if view_exists:
            logger.debug(f"[TABLE_MGT] Vue splus.{table_name_lower} existe déjà")
            return

        # Crée la vue
        logger.info(f"[TABLE_MGT] Création vue splus.{table_name_lower} -> {self.target_schema}.{table_name_lower}")
        success = self.view_switcher.create_view_for_table(table_name_lower, self.target_schema)

        if success:
            logger.info(f"[TABLE_MGT] Vue splus.{table_name_lower} créée avec succès")
        else:
            logger.warning(f"[TABLE_MGT] Échec création vue splus.{table_name_lower}")

    def _get_qualified_table_name(self, table_name: str) -> str:
        """
        Retourne le nom de table qualifié avec le schéma.

        Args:
            table_name: Nom de la table

        Returns:
            Nom qualifié (ex: 'splus_blue.csks' ou 'csks' si pas de target_schema)
        """
        return self._schema_qualifier.qualify(table_name)

    def ensure_meta_columns(self, table_name: str) -> None:
        """
        S'assure que les meta colonnes _source et _imported_at existent.

        Cette méthode ajoute les colonnes si elles n'existent pas déjà,
        permettant ainsi la migration des tables existantes.

        Args:
            table_name: Nom de la table à mettre à jour
        """
        qualified_name = self._get_qualified_table_name(table_name)
        logger.info(f"[TABLE_MGT] Vérification meta colonnes pour {qualified_name}")

        sql = f"""
            ALTER TABLE {qualified_name}
            ADD COLUMN IF NOT EXISTS _source VARCHAR(50) DEFAULT '{self.default_source}';

            ALTER TABLE {qualified_name}
            ADD COLUMN IF NOT EXISTS _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        """

        try:
            self.postgres_hook.run(sql)
            logger.info(f"[TABLE_MGT] Meta colonnes OK pour {qualified_name}")
        except Exception as e:
            logger.warning(f"[TABLE_MGT] Erreur ajout meta colonnes pour {qualified_name}: {e}")

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

        logger.info(f"[TABLE_MGT] Table: {table_name}")
        logger.info(f"[TABLE_MGT] Environment: {self.environment}")
        logger.info(f"[TABLE_MGT] Exists: {exists}")

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
            AMUESchemaError: Si structure invalide
        """
        required_fields = ['table_name', 'columns', 'primary_keys', 'exists']
        missing = [f for f in required_fields if f not in structure_info]

        if missing:
            raise AMUESchemaError(
                f"Structure invalide pour table. Champs manquants: {missing}",
                table_name=structure_info.get('table_name', 'unknown')
            )

        if not structure_info['columns']:
            raise AMUESchemaError(
                f"Table {structure_info['table_name']}: aucune colonne définie",
                table_name=structure_info['table_name']
            )

    def _handle_production_table(self, structure_info: Dict, exists: bool) -> Dict:
        """
        Gère une table en environnement de production

        En production, on refuse catégoriquement toute création.
        L'ajout de meta colonnes est autorisé (opération non destructive).
        """
        table_name = structure_info['table_name'].lower()
        qualified_name = self._get_qualified_table_name(table_name)

        if not exists:
            raise AMUETableNotFoundError(
                f"[PRODUCTION] Table {qualified_name} inexistante. "
                "Création interdite en production. Créez la table manuellement.",
                table_name=table_name
            )

        logger.info(f"[PRODUCTION] Utilisation table existante: {qualified_name}")
        # S'assure que les meta colonnes existent (ADD COLUMN IF NOT EXISTS est safe)
        self.ensure_meta_columns(table_name)
        # S'assure que la vue existe dans splus (mode blue/green)
        self._ensure_view_exists(table_name)
        return self._build_existing_table_result(structure_info)

    def _handle_dev_table(self, structure_info: Dict, exists: bool) -> Dict:
        """Gère une table en environnement de développement"""
        table_name = structure_info['table_name'].lower()
        qualified_name = self._get_qualified_table_name(table_name)

        if exists:
            logger.info(f"[DEV] Utilisation table existante: {qualified_name}")
            # S'assure que les meta colonnes existent
            self.ensure_meta_columns(table_name)
            # S'assure que la vue existe dans splus (mode blue/green)
            self._ensure_view_exists(table_name)
            return self._build_existing_table_result(structure_info)

        logger.info(f"[DEV] Création de la table: {qualified_name}")
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
        qualified_name = self._get_qualified_table_name(table_name)
        columns = structure_info['columns']
        primary_keys_str = structure_info['primary_keys']

        try:
            # Génère le DDL avec le nom qualifié
            create_sql = self._build_create_table_sql(
                qualified_name,
                columns,
                primary_keys_str
            )

            # Exécute la création
            logger.info(f"[DEV] Exécution CREATE TABLE {qualified_name}")
            self.postgres_hook.run(create_sql)
            logger.info(f"[DEV] Table {qualified_name} créée avec succès")

            # S'assure que la vue existe dans splus (mode blue/green)
            self._ensure_view_exists(table_name)

            # Construit le résultat
            result = TableManagementResult(
                table_name=table_name,
                columns=[col['name'].lower() for col in columns],
                primary_keys=primary_keys_str,
                created=True,
                status='success'
            )

            return self._result_to_dict(result)

        except (DatabaseError, IntegrityError, ProgrammingError) as e:
            error_msg = f"Échec création table {table_name} (erreur SQL): {str(e)}"
            logger.error(f"[ERROR] {error_msg}")
            raise AMUEDatabaseError(error_msg, table_name=table_name) from e
        except ValueError as e:
            error_msg = f"Échec création table {table_name} (paramètres invalides): {str(e)}"
            logger.error(f"[ERROR] {error_msg}")
            raise AMUESchemaError(error_msg, table_name=table_name) from e

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

        # Ajout des meta colonnes pour le traçage
        column_defs.append(f"_source VARCHAR(50) DEFAULT '{self.default_source}'")
        column_defs.append("_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

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
            logger.warning("[WARN] Aucune clé primaire définie")
            return ''

        pk_list = [
            pk.strip().lower()
            for pk in primary_keys_str.split(',')
            if pk.strip()
        ]

        if not pk_list:
            return ''

        pk_cols = ', '.join(pk_list)
        logger.info(f"[TABLE_MGT] Clés primaires: {pk_cols}")

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
