"""
Layer: infrastructure

Requêtes d'introspection PostgreSQL (information_schema) : existence de
tables/vues/schémas.
"""
from typing import List

from common.domain.interfaces import SqlExecutor


def list_tables(hook: SqlExecutor, schema: str) -> List[str]:
    """
    Liste les tables BASE TABLE d'un schéma PostgreSQL.

    Args:
        hook: Hook PostgreSQL (doit exposer `get_records`)
        schema: Nom du schéma (ex: 'splus_blue')

    Returns:
        Liste de noms de tables triés alphabétiquement
    """
    result = hook.get_records(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        parameters=(schema,),
    )
    return [row[0] for row in result] if result else []


def list_views(hook: SqlExecutor, schema: str) -> List[str]:
    """
    Liste les vues d'un schéma PostgreSQL.

    Args:
        hook: Hook PostgreSQL
        schema: Nom du schéma (ex: 'splus')

    Returns:
        Liste de noms de vues triés alphabétiquement
    """
    result = hook.get_records(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        parameters=(schema,),
    )
    return [row[0] for row in result] if result else []


def table_exists(hook: SqlExecutor, schema: str, table: str) -> bool:
    """
    Vérifie si une table existe dans un schéma PostgreSQL.

    Args:
        hook: Hook PostgreSQL
        schema: Nom du schéma
        table: Nom de la table (insensible à la casse)

    Returns:
        True si la table existe
    """
    result = hook.get_first(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        )
        """,
        parameters=(schema, table.lower()),
    )
    return result[0] if result else False


def schema_exists(hook: SqlExecutor, schema: str) -> bool:
    """
    Vérifie si un schéma PostgreSQL existe.

    Args:
        hook: Hook PostgreSQL
        schema: Nom du schéma

    Returns:
        True si le schéma existe
    """
    rows = hook.get_records(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        parameters=(schema,),
    )
    return bool(rows)
