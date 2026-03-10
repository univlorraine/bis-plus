"""Tests unitaires pour schema_utils — SchemaQualifier et helpers information_schema."""
import pytest
from unittest.mock import MagicMock
from psycopg2 import sql

from amue.utils.database.schema_utils import (
    SchemaQualifier,
    list_tables,
    list_views,
    table_exists,
    schema_exists,
)


# =============================================================================
# SchemaQualifier
# =============================================================================

class TestSchemaQualifierQualify:
    def test_qualify_with_schema(self):
        q = SchemaQualifier('splus_blue')
        assert q.qualify('CSKS') == 'splus_blue.csks'

    def test_qualify_without_schema(self):
        q = SchemaQualifier()
        assert q.qualify('CSKS') == 'csks'

    def test_qualify_lowercase(self):
        q = SchemaQualifier('splus_green')
        assert q.qualify('PRPS') == 'splus_green.prps'

    def test_qualify_already_lowercase(self):
        q = SchemaQualifier('splus_blue')
        assert q.qualify('csks') == 'splus_blue.csks'


class TestSchemaQualifierQualifyIdentifier:
    def test_qualify_identifier_with_schema(self):
        q = SchemaQualifier('splus_blue')
        result = q.qualify_identifier('csks')
        assert isinstance(result, sql.Composable)

    def test_qualify_identifier_without_schema(self):
        q = SchemaQualifier()
        result = q.qualify_identifier('csks')
        assert isinstance(result, sql.Identifier)


class TestSchemaQualifierUnqualify:
    def test_unqualify_qualified_name(self):
        q = SchemaQualifier('splus_blue')
        assert q.unqualify('splus_blue.csks') == 'csks'

    def test_unqualify_bare_name(self):
        q = SchemaQualifier('splus_blue')
        assert q.unqualify('csks') == 'csks'


class TestSchemaQualifierIsQualified:
    def test_is_qualified_true(self):
        q = SchemaQualifier()
        assert q.is_qualified('splus_blue.csks') is True

    def test_is_qualified_false(self):
        q = SchemaQualifier()
        assert q.is_qualified('csks') is False


class TestSchemaQualifierSetter:
    def test_set_schema(self):
        q = SchemaQualifier('splus_blue')
        q.target_schema = 'splus_green'
        assert q.qualify('csks') == 'splus_green.csks'

    def test_set_none(self):
        q = SchemaQualifier('splus_blue')
        q.target_schema = None
        assert q.qualify('csks') == 'csks'


# =============================================================================
# list_tables
# =============================================================================

class TestListTables:
    def test_returns_table_names(self):
        hook = MagicMock()
        hook.get_records.return_value = [('csks',), ('prps',), ('wbs',)]
        result = list_tables(hook, 'splus_blue')
        assert result == ['csks', 'prps', 'wbs']

    def test_empty_schema(self):
        hook = MagicMock()
        hook.get_records.return_value = []
        result = list_tables(hook, 'splus_blue')
        assert result == []

    def test_none_result(self):
        hook = MagicMock()
        hook.get_records.return_value = None
        result = list_tables(hook, 'splus_blue')
        assert result == []

    def test_passes_correct_schema(self):
        hook = MagicMock()
        hook.get_records.return_value = []
        list_tables(hook, 'splus_green')
        call_args = hook.get_records.call_args
        assert 'splus_green' in call_args[1].get('parameters', call_args[0][1] if len(call_args[0]) > 1 else ())


# =============================================================================
# list_views
# =============================================================================

class TestListViews:
    def test_returns_view_names(self):
        hook = MagicMock()
        hook.get_records.return_value = [('csks',), ('prps',)]
        result = list_views(hook, 'splus')
        assert result == ['csks', 'prps']

    def test_empty(self):
        hook = MagicMock()
        hook.get_records.return_value = []
        result = list_views(hook, 'splus')
        assert result == []


# =============================================================================
# table_exists
# =============================================================================

class TestTableExists:
    def test_table_present(self):
        hook = MagicMock()
        hook.get_first.return_value = (True,)
        assert table_exists(hook, 'splus_blue', 'csks') is True

    def test_table_absent(self):
        hook = MagicMock()
        hook.get_first.return_value = (False,)
        assert table_exists(hook, 'splus_blue', 'unknown') is False

    def test_none_result(self):
        hook = MagicMock()
        hook.get_first.return_value = None
        assert table_exists(hook, 'splus_blue', 'csks') is False

    def test_table_name_lowercased(self):
        hook = MagicMock()
        hook.get_first.return_value = (True,)
        table_exists(hook, 'splus_blue', 'CSKS')
        call_args = hook.get_first.call_args
        params = call_args[1].get('parameters', call_args[0][1] if len(call_args[0]) > 1 else ())
        assert 'csks' in params


# =============================================================================
# schema_exists
# =============================================================================

class TestSchemaExists:
    def test_schema_present(self):
        hook = MagicMock()
        hook.get_records.return_value = [(1,)]
        assert schema_exists(hook, 'splus_blue') is True

    def test_schema_absent(self):
        hook = MagicMock()
        hook.get_records.return_value = []
        assert schema_exists(hook, 'nonexistent') is False
