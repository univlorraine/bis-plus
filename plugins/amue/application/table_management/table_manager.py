"""
Layer: application

Gestionnaire de création et mise à jour des tables PostgreSQL.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module gère le schéma DDL (Data Definition Language) des tables AMUE dans
PostgreSQL. La décision de créer ou réutiliser une table se base uniquement
sur son existence en base :

┌────────────────────┬────────────────────────────────────┐
│ Table existe       │ Action                             │
├────────────────────┼────────────────────────────────────┤
│ Oui                │ Utilisation de la table existante  │
│ Non                │ Création automatique               │
└────────────────────┴────────────────────────────────────┘

PHILOSOPHIE :
    - La structure est toujours validée avant toute opération
    - Si la table n'existe pas, elle est créée automatiquement
    - Support blue/green : création dans le schéma cible spécifié

================================================================================
GÉNÉRATION DDL
================================================================================

Le DDL généré inclut :
    - CREATE TABLE avec colonnes typées
    - Contrainte PRIMARY KEY si clés définies

Exemple de DDL généré :
    CREATE TABLE csks (
        bukrs VARCHAR(4),
        kostl VARCHAR(10),
        datab DATE,
        PRIMARY KEY (bukrs, kostl)
    );

================================================================================
CONFIGURATION
================================================================================

Connexion PostgreSQL :
    - postgres_conn_id : "postgres_data"
    - schema : "splus"

================================================================================
"""
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from common.domain.interfaces import SqlExecutor
from psycopg2 import DatabaseError, IntegrityError, ProgrammingError

from amue.domain.exceptions import AMUESchemaError, AMUEDatabaseError
from common.application.table_creator import build_meta_column_defs
from common.infrastructure.database.hooks import resolve_postgres_hook
from common.infrastructure.database.identifier_qualifier import SchemaQualifier
from common.domain.protected_source import PROTECTED_SOURCE

_SAFE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')  # noms qualifiés (schéma.table)
_SAFE_COLUMN_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')      # colonnes et PKs : pas de point

# Whitelist des types PostgreSQL autorisés en DDL.
# Les types proviennent de la réponse API (champ type_postgres) — tout autre
# token serait potentiellement du SQL injecté.
_ALLOWED_PG_TYPE_RE = re.compile(
    r'^('
    r'TEXT|BOOLEAN|BYTEA|DATE|UUID|JSON|JSONB|INTERVAL'
    r'|SMALLINT|INTEGER|INT|BIGINT'
    r'|REAL|FLOAT8?|DOUBLE\s+PRECISION'
    r'|NUMERIC|DECIMAL'
    r'|(?:NUMERIC|DECIMAL)\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\)'
    r'|(?:VARCHAR|CHAR(?:ACTER)?(?:\s+VARYING)?)\s*(?:\(\s*\d+\s*\))?'
    r'|TIMESTAMP(?:\s+(?:WITH|WITHOUT)\s+TIME\s+ZONE)?'
    r'|TIME(?:\s+(?:WITH|WITHOUT)\s+TIME\s+ZONE)?'
    r')$',
    re.IGNORECASE,
)

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
    - Si la table existe : utilisation de la table existante
    - Si la table n'existe pas : création automatique
    - Validation systématique de la structure avant opération
    - Support blue/green : création dans le schéma cible spécifié
    """

    def __init__(self, postgres_hook: SqlExecutor = None, target_schema: Optional[str] = None):
        """
        Initialise le gestionnaire de tables

        Args:
            postgres_hook: Hook PostgreSQL personnalisé (optionnel)
            target_schema: Schéma cible pour blue/green (ex: 'splus_blue')
                          Si None, utilise le schéma par défaut 'splus'
        """
        self._schema_qualifier = SchemaQualifier(target_schema)
        self.postgres_hook = resolve_postgres_hook(postgres_hook, target_schema)
        self.default_source = PROTECTED_SOURCE

    @property
    def target_schema(self) -> Optional[str]:
        """Retourne le schéma cible."""
        return self._schema_qualifier.target_schema

    @target_schema.setter
    def target_schema(self, value: Optional[str]) -> None:
        """Définit le schéma cible."""
        self._schema_qualifier.target_schema = value

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
        if not _SAFE_COLUMN_RE.match(table_name):
            raise AMUESchemaError(
                f"Nom de table non sécurisé pour ensure_meta_columns: {table_name!r}",
                table_name=table_name,
            )
        qualified_name = self._get_qualified_table_name(table_name)
        if not _SAFE_IDENTIFIER_RE.match(qualified_name):
            raise AMUESchemaError(f"Nom de table non sécurisé pour le DDL : {qualified_name!r}")
        logger.info(f"[TABLE_MGT] Vérification meta colonnes pour {qualified_name}")

        alter_sql = (
            f"ALTER TABLE {qualified_name} "
            "ADD COLUMN IF NOT EXISTS _source VARCHAR(50) DEFAULT %s, "
            "ADD COLUMN IF NOT EXISTS _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )

        try:
            self.postgres_hook.run(alter_sql, parameters=(self.default_source,))
            logger.info(f"[TABLE_MGT] Meta colonnes OK pour {qualified_name}")
        except Exception as e:
            logger.warning(f"[TABLE_MGT] Erreur ajout meta colonnes pour {qualified_name}: {e}")

    def manage_table(self, structure_info: Dict) -> Dict:
        """
        Point d'entrée principal pour la gestion d'une table.

        Décision basée uniquement sur l'existence de la table en base :
        - Table existe : utilisation + vérification meta colonnes
        - Table absente : création automatique

        Args:
            structure_info: Informations de structure de la table

        Returns:
            Dictionnaire avec résultat de l'opération
        """
        table_name = structure_info['table_name']
        exists = structure_info['exists']

        logger.info(f"[TABLE_MGT] Table: {table_name}")
        logger.info(f"[TABLE_MGT] Exists: {exists}")

        # Validation de la structure
        self._validate_structure_info(structure_info)

        table_name_lower = table_name.lower()
        qualified_name = self._get_qualified_table_name(table_name_lower)

        if exists:
            logger.info(f"[TABLE_MGT] Utilisation table existante: {qualified_name}")
            self.ensure_meta_columns(table_name_lower)
            return self._build_existing_table_result(structure_info)

        logger.info(f"[TABLE_MGT] Création de la table: {qualified_name}")
        return self._create_table(structure_info)

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
            AMUEDatabaseError: Si création échoue (erreur SQL)
            AMUESchemaError: Si paramètres invalides
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
            logger.info(f"[TABLE_MGT] Exécution CREATE TABLE {qualified_name}")
            self.postgres_hook.run(create_sql)
            logger.info(f"[TABLE_MGT] Table {qualified_name} créée avec succès")

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
        # 1) Valider le nom de table en premier — avant tout traitement
        if not _SAFE_IDENTIFIER_RE.match(table_name):
            raise AMUESchemaError(f"Nom de table non sécurisé pour le DDL : {table_name!r}")

        # 2) Valider chaque colonne (nom + type) puis construire les définitions
        column_defs = []
        for col in columns:
            col_name = col['name'].lower()
            col_type = col['type_postgres'].strip()

            if not _SAFE_COLUMN_RE.match(col_name):
                raise AMUESchemaError(
                    f"Nom de colonne non sécurisé: {col_name!r}",
                    table_name=table_name,
                )
            if not _ALLOWED_PG_TYPE_RE.match(col_type):
                raise AMUESchemaError(
                    f"Type PostgreSQL non autorisé pour la colonne '{col_name}': {col_type!r}",
                    table_name=table_name,
                )
            column_defs.append(f"{col_name} {col_type}")

        # 3) Meta colonnes (constantes de confiance, pas de données API)
        column_defs.extend(build_meta_column_defs(self.default_source))

        # 4) Contrainte PRIMARY KEY (avec validation des noms)
        pk_constraint = self._build_primary_key_constraint(primary_keys_str, table_name)

        columns_sql = ',\n    '.join(column_defs)

        create_sql = f"""
            DROP TABLE IF EXISTS {table_name} CASCADE;

            CREATE TABLE {table_name} (
                {columns_sql}{pk_constraint}
            );
        """

        return create_sql

    def _build_primary_key_constraint(
        self, primary_keys_str: str, table_name: str = 'unknown'
    ) -> str:
        """
        Construit la clause PRIMARY KEY

        Args:
            primary_keys_str: Clés primaires CSV
            table_name: Nom de table utilisé dans les messages d'erreur

        Returns:
            Clause SQL ou chaîne vide si pas de PK
        """
        if not primary_keys_str or not primary_keys_str.strip():
            logger.warning("[WARN] Aucune clé primaire définie")
            return ''

        pk_list = []
        for pk in primary_keys_str.split(','):
            pk_name = pk.strip().lower()
            if not pk_name:
                continue
            if not _SAFE_COLUMN_RE.match(pk_name):
                raise AMUESchemaError(
                    f"Nom de clé primaire non sécurisé: {pk_name!r}",
                    table_name=table_name,
                )
            pk_list.append(pk_name)

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
