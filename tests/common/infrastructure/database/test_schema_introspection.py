"""Tests unitaires pour schema_introspection — list_tables, list_views, table_exists, schema_exists."""
from unittest.mock import MagicMock

from common.infrastructure.database.schema_introspection import (
    list_tables,
    list_views,
    table_exists,
    schema_exists,
)


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


class TestSchemaExists:
    def test_schema_present(self):
        hook = MagicMock()
        hook.get_records.return_value = [(1,)]
        assert schema_exists(hook, 'splus_blue') is True

    def test_schema_absent(self):
        hook = MagicMock()
        hook.get_records.return_value = []
        assert schema_exists(hook, 'nonexistent') is False
