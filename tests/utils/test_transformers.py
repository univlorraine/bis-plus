"""
Tests unitaires pour les fonctions de transformation
"""
import pytest

from amue.utils.transformers import parse_column_definition
from common.utils.fingerprint import (
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints,
)
from common.utils.validators import (
    validate_table_name,
    validate_column_name,
    validate_identifier,
)


class TestParseColumnDefinition:
    """Tests pour parse_column_definition"""

    # --- Types texte ---

    def test_text(self):
        """TEXT reste TEXT"""
        assert parse_column_definition('TEXT') == 'TEXT'

    def test_clob_to_text(self):
        """CLOB doit être converti en TEXT"""
        assert parse_column_definition('CLOB') == 'TEXT'

    def test_varchar(self):
        """VARCHAR conserve ses paramètres"""
        assert parse_column_definition('VARCHAR(50)') == 'VARCHAR(50)'
        assert parse_column_definition('VARCHAR(255)') == 'VARCHAR(255)'

    def test_nvarchar_to_varchar(self):
        """NVARCHAR doit être converti en VARCHAR"""
        assert parse_column_definition('NVARCHAR(100)') == 'VARCHAR(100)'

    def test_char_to_char(self):
        """CHAR doit être converti en CHAR"""
        assert parse_column_definition('CHAR(10)') == 'CHAR(10)'

    def test_character_to_char(self):
        """CHARACTER doit être converti en CHAR"""
        assert parse_column_definition('CHARACTER(20)') == 'CHAR(20)'

    def test_nchar_to_char(self):
        """NCHAR doit être converti en CHAR"""
        assert parse_column_definition('NCHAR(10)') == 'CHAR(10)'

    # --- Types entiers ---

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

    def test_tinyint_to_smallint(self):
        """TINYINT doit devenir SMALLINT"""
        assert parse_column_definition('TINYINT') == 'SMALLINT'

    def test_mediumint_to_integer(self):
        """MEDIUMINT doit devenir INTEGER"""
        assert parse_column_definition('MEDIUMINT') == 'INTEGER'

    def test_int2_to_smallint(self):
        """INT2 doit devenir SMALLINT"""
        assert parse_column_definition('INT2') == 'SMALLINT'

    def test_int8_to_bigint(self):
        """INT8 doit devenir BIGINT"""
        assert parse_column_definition('INT8') == 'BIGINT'

    # --- Types numériques ---

    def test_numeric(self):
        """NUMERIC conserve ses paramètres"""
        assert parse_column_definition('NUMERIC(10,2)') == 'NUMERIC(10,2)'
        assert parse_column_definition('NUMERIC(5)') == 'NUMERIC(5)'

    def test_decimal_to_numeric(self):
        """DECIMAL doit être converti en NUMERIC"""
        assert parse_column_definition('DECIMAL(10,2)') == 'NUMERIC(10,2)'

    def test_boolean(self):
        """BOOLEAN reste BOOLEAN"""
        assert parse_column_definition('BOOLEAN') == 'BOOLEAN'

    # --- Types réels ---

    def test_real_to_double_precision(self):
        """REAL doit devenir DOUBLE PRECISION"""
        assert parse_column_definition('REAL') == 'DOUBLE PRECISION'

    def test_double_to_double_precision(self):
        """DOUBLE doit devenir DOUBLE PRECISION"""
        assert parse_column_definition('DOUBLE') == 'DOUBLE PRECISION'

    def test_float_to_double_precision(self):
        """FLOAT doit devenir DOUBLE PRECISION"""
        assert parse_column_definition('FLOAT') == 'DOUBLE PRECISION'

    # --- Types date ---

    def test_date_to_timestamp(self):
        """DATE doit être converti en TIMESTAMP"""
        assert parse_column_definition('DATE') == 'TIMESTAMP'

    def test_datetime_to_timestamp(self):
        """DATETIME doit être converti en TIMESTAMP"""
        assert parse_column_definition('DATETIME') == 'TIMESTAMP'

    # --- Types binaires ---

    def test_blob_to_bytea(self):
        """BLOB doit être converti en BYTEA"""
        assert parse_column_definition('BLOB') == 'BYTEA'

    # --- Cas limites ---

    def test_unknown_type_passes_through(self):
        """Un type inconnu passe tel quel"""
        assert parse_column_definition('UNKNOWN_TYPE') == 'UNKNOWN_TYPE'
        assert parse_column_definition('') == 'TEXT'

    def test_case_insensitive(self):
        """La conversion doit être insensible à la casse"""
        assert parse_column_definition('varchar(50)') == 'VARCHAR(50)'
        assert parse_column_definition('Varchar(50)') == 'VARCHAR(50)'
        assert parse_column_definition('integer(1)') == 'SMALLINT'
        assert parse_column_definition('blob') == 'BYTEA'


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

    def test_compute_hash_with_type_original(self):
        """Le hash change quand on utilise type_original vs type_postgres"""
        columns = [
            {'name': 'id', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'name', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        hash_pg = compute_structure_hash_with_pk(columns, 'id', type_key='type_postgres')
        hash_orig = compute_structure_hash_with_pk(columns, 'id', type_key='type_original')
        assert hash_pg != hash_orig

    def test_compute_hash_type_original_consistent(self):
        """Même structure avec type_original donne le même hash"""
        columns = [
            {'name': 'id', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
        ]
        hash1 = compute_structure_hash_with_pk(columns, 'id', type_key='type_original')
        hash2 = compute_structure_hash_with_pk(columns, 'id', type_key='type_original')
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
