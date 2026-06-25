"""
Configuration pytest globale pour les tests AMUE.

Le pythonpath est configuré dans pytest.ini (pythonpath = plugins),
ce qui rend amue importable directement sans sys.path hacks.
"""
import json
from pathlib import Path

import pytest
from unittest.mock import patch


_VARIABLES_PATH = Path(__file__).parent.parent / 'config' / 'airflow_variables.json'

with _VARIABLES_PATH.open(encoding='utf-8') as _f:
    _TEST_TYPE_MAPPING = json.load(_f)['TYPE_MAPPING_SQLITE_TO_POSTGRES']


@pytest.fixture(autouse=True)
def reset_type_mapping_cache():
    """Reset le cache du type mapping avant chaque test."""
    import amue.domain.transformers as mod
    mod._type_mapping_cache = None
    yield
    mod._type_mapping_cache = None


@pytest.fixture(autouse=True)
def mock_type_mapping_variable():
    """Mock la variable Airflow TYPE_MAPPING_SQLITE_TO_POSTGRES pour les tests."""
    with patch('amue.domain.transformers.VarMgr') as mock_varmgr:
        mock_varmgr.get.return_value = json.dumps(_TEST_TYPE_MAPPING)
        yield mock_varmgr
