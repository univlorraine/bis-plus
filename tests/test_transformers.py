"""
Tests unitaires pour les fonctions de transformation
"""
import pytest
import sys
import os

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from amue.utils.transformers import (
    parse_column_definition,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints,
    validate_table_name,
    validate_column_name,
    validate_identifier,
)


class TestParseColumnDefinition:
    """Tests pour parse_column_definition"""

    def test_varchar2_to_varchar(self):
        """VARCHAR2 Oracle doit être converti en VARCHAR PostgreSQL"""
        assert parse_column_definition('VARCHAR2(50)') == 'VARCHAR(50)'
        assert parse_column_definition('VARCHAR2(255)') == 'VARCHAR(255)'

    def test_number_to_numeric(self):
        """NUMBER Oracle doit être converti en NUMERIC PostgreSQL"""
        assert parse_column_definition('NUMBER(10,2)') == 'NUMERIC(10,2)'
        assert parse_column_definition('NUMBER(5)') == 'NUMERIC(5)'

    def test_date_to_timestamp(self):
        """DATE Oracle doit être converti en TIMESTAMP PostgreSQL"""
        assert parse_column_definition('DATE') == 'TIMESTAMP'

    def test_char_to_bpchar(self):
        """CHAR doit être converti en BPCHAR PostgreSQL"""
        assert parse_column_definition('CHAR(10)') == 'BPCHAR(10)'

    def test_clob_to_text(self):
        """CLOB doit être converti en TEXT"""
        assert parse_column_definition('CLOB') == 'TEXT'

    def test_blob_to_bytea(self):
        """BLOB doit être converti en BYTEA"""
        assert parse_column_definition('BLOB') == 'BYTEA'

    def test_integer_small(self):
        """INTEGER(1-2) doit devenir SMALLINT"""
        assert parse_column_definition('INTEGER(1)') == 'SMALLINT'
        assert parse_column_definition('INTEGER(2)') == 'SMALLINT'

    def test_integer_medium(self):
        """INTEGER(3-4) doit rester INTEGER"""
        assert parse_column_definition('INTEGER(3)') == 'INTEGER'
        assert parse_column_definition('INTEGER(4)') == 'INTEGER'

    def test_integer_large(self):
        """INTEGER(5+) doit devenir BIGINT"""
        assert parse_column_definition('INTEGER(5)') == 'BIGINT'
        assert parse_column_definition('INTEGER(10)') == 'BIGINT'

    def test_invalid_type_returns_text(self):
        """Un type invalide doit retourner TEXT par défaut"""
        assert parse_column_definition('INVALID_TYPE') == 'INVALID_TYPE'
        assert parse_column_definition('') == 'TEXT'

    def test_case_insensitive(self):
        """La conversion doit être insensible à la casse"""
        assert parse_column_definition('varchar2(50)') == 'VARCHAR(50)'
        assert parse_column_definition('Varchar2(50)') == 'VARCHAR(50)'


class TestValidateTableName:
    """Tests pour validate_table_name"""

    def test_valid_table_name(self):
        """Noms de table valides"""
        assert validate_table_name('csks') == 'CSKS'
        assert validate_table_name('MY_TABLE') == 'MY_TABLE'
        assert validate_table_name('table123') == 'TABLE123'

    def test_empty_table_name(self):
        """Nom de table vide doit lever une erreur"""
        with pytest.raises(ValueError, match="ne peut pas être vide"):
            validate_table_name('')

    def test_invalid_characters(self):
        """Caractères invalides doivent lever une erreur"""
        with pytest.raises(ValueError, match="invalide"):
            validate_table_name('DROP TABLE users--')

        with pytest.raises(ValueError, match="invalide"):
            validate_table_name('table; DELETE')

        with pytest.raises(ValueError, match="invalide"):
            validate_table_name("table'name")

    def test_too_long_name(self):
        """Nom trop long doit lever une erreur"""
        with pytest.raises(ValueError, match="invalide"):
            validate_table_name('a' * 64)


class TestValidateColumnName:
    """Tests pour validate_column_name"""

    def test_valid_column_name(self):
        """Noms de colonne valides"""
        assert validate_column_name('MY_COLUMN') == 'my_column'
        assert validate_column_name('id') == 'id'
        assert validate_column_name('column_123') == 'column_123'

    def test_empty_column_name(self):
        """Nom de colonne vide doit lever une erreur"""
        with pytest.raises(ValueError, match="ne peut pas être vide"):
            validate_column_name('')

    def test_invalid_characters(self):
        """Caractères invalides doivent lever une erreur"""
        with pytest.raises(ValueError, match="invalide"):
            validate_column_name('column; DROP')


class TestValidateIdentifier:
    """Tests pour validate_identifier"""

    def test_valid_identifier(self):
        """Identifiants valides"""
        assert validate_identifier('schema_name', 'schema') == 'schema_name'

    def test_invalid_identifier(self):
        """Identifiants invalides"""
        with pytest.raises(ValueError):
            validate_identifier('invalid-name!', 'identifier')


class TestComputeStructureHashWithPk:
    """Tests pour compute_structure_hash_with_pk"""

    def test_same_structure_same_hash(self):
        """Même structure doit donner le même hash"""
        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
        ]
        hash1 = compute_structure_hash_with_pk(columns, 'id')
        hash2 = compute_structure_hash_with_pk(columns, 'id')
        assert hash1 == hash2

    def test_different_pk_different_hash(self):
        """Clés primaires différentes doivent donner des hash différents"""
        columns = [
            {'name': 'id', 'type_postgres': 'INTEGER'},
            {'name': 'name', 'type_postgres': 'VARCHAR(50)'}
        ]
        hash1 = compute_structure_hash_with_pk(columns, 'id')
        hash2 = compute_structure_hash_with_pk(columns, 'id,name')
        assert hash1 != hash2

    def test_different_columns_different_hash(self):
        """Colonnes différentes doivent donner des hash différents"""
        columns1 = [{'name': 'id', 'type_postgres': 'INTEGER'}]
        columns2 = [{'name': 'id', 'type_postgres': 'BIGINT'}]
        hash1 = compute_structure_hash_with_pk(columns1, 'id')
        hash2 = compute_structure_hash_with_pk(columns2, 'id')
        assert hash1 != hash2

    def test_no_primary_key(self):
        """Sans clé primaire, le hash doit être calculé avec NO_PRIMARY_KEY"""
        columns = [{'name': 'id', 'type_postgres': 'INTEGER'}]
        hash1 = compute_structure_hash_with_pk(columns, '')
        hash2 = compute_structure_hash_with_pk(columns)
        assert hash1 == hash2


class TestFormatPrimaryKeys:
    """Tests pour format_primary_keys"""

    def test_single_key(self):
        """Une seule clé"""
        assert format_primary_keys('id') == ['id']

    def test_multiple_keys(self):
        """Plusieurs clés"""
        assert format_primary_keys('id, name, date') == ['id', 'name', 'date']

    def test_uppercase_to_lowercase(self):
        """Les clés doivent être en minuscules"""
        assert format_primary_keys('ID, Name, DATE') == ['id', 'name', 'date']

    def test_empty_string(self):
        """Chaîne vide doit retourner liste vide"""
        assert format_primary_keys('') == []

    def test_whitespace_handling(self):
        """Les espaces doivent être gérés"""
        assert format_primary_keys('  id  ,  name  ') == ['id', 'name']


class TestCompareFingerprints:
    """Tests pour compare_fingerprints"""

    def test_same_fingerprints(self):
        """Fingerprints identiques"""
        result = compare_fingerprints('abc123', 'abc123', 'TEST')
        assert result['changed'] is False
        assert result['old'] == 'abc123'
        assert result['new'] == 'abc123'

    def test_different_fingerprints(self):
        """Fingerprints différents"""
        result = compare_fingerprints('abc123', 'def456', 'TEST')
        assert result['changed'] is True
        assert result['old'] == 'abc123'
        assert result['new'] == 'def456'
        assert result['table_name'] == 'TEST'
