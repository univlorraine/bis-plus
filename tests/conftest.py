"""
Configuration pytest globale pour les tests AMUE.

Le pythonpath est configuré dans pytest.ini (pythonpath = plugins),
ce qui rend amue importable directement sans sys.path hacks.
"""
import json
import pytest
from unittest.mock import patch


# Mapping complet utilisé par les tests (identique à la variable Airflow attendue)
_TEST_TYPE_MAPPING = {
    "TEXT": "TEXT",
    "CLOB": "TEXT",
    "CHAR": "BPCHAR",
    "CHARACTER": "BPCHAR",
    "VARCHAR": "VARCHAR",
    "NCHAR": "BPCHAR",
    "NVARCHAR": "VARCHAR",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "TINYINT": "SMALLINT",
    "SMALLINT": "SMALLINT",
    "MEDIUMINT": "INTEGER",
    "BIGINT": "BIGINT",
    "INT2": "SMALLINT",
    "INT8": "BIGINT",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "BOOLEAN": "BOOLEAN",
    "REAL": "DOUBLE PRECISION",
    "DOUBLE": "DOUBLE PRECISION",
    "FLOAT": "DOUBLE PRECISION",
    "DATE": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "BLOB": "BYTEA",
}


@pytest.fixture(autouse=True)
def reset_type_mapping_cache():
    """Reset le cache du type mapping avant chaque test."""
    import amue.utils.transformers as mod
    mod._type_mapping_cache = None
    yield
    mod._type_mapping_cache = None


@pytest.fixture(autouse=True)
def mock_type_mapping_variable():
    """Mock la variable Airflow TYPE_MAPPING_SQLITE_TO_POSTGRES pour les tests."""
    with patch('amue.utils.transformers.VarMgr') as mock_varmgr:
        mock_varmgr.get.return_value = json.dumps(_TEST_TYPE_MAPPING)
        yield mock_varmgr
