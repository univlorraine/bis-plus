"""Tests unitaires pour identifier_qualifier — SchemaQualifier."""
from psycopg2 import sql

from common.infrastructure.database.identifier_qualifier import SchemaQualifier


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
