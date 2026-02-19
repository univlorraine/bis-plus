"""Tests pour la task validate_tables."""
import pytest
from unittest.mock import patch
from airflow.exceptions import AirflowException


class TestValidateTablesErrorDetails:
    """Tests pour les détails d'erreur dans validate_tables"""

    @patch('amue.tasks.import_dag.validate_tables.logger')
    def test_validate_tables_error_includes_details(self, mock_logger):
        """Le message d'exception inclut les détails par table"""
        from amue.tasks.import_dag.validate_tables import validate_tables

        results = [
            {
                'table_name': 'CSKS',
                'status': 'error',
                'phase': 'fingerprint',
                'error': 'CHANGEMENT DE STRUCTURE DETECTE pour CSKS'
            },
            {
                'table_name': 'ANLA',
                'status': 'error',
                'phase': 'status',
                'error': "Table ANLA status=KO (attendu: OK)"
            },
            {
                'table_name': 'BSEG',
                'status': 'success',
                'phase': 'complete',
                'error': None
            }
        ]

        with pytest.raises(AirflowException) as exc_info:
            validate_tables.function(results)

        error_msg = str(exc_info.value)
        assert '2 table(s) en erreur' in error_msg
        assert 'CSKS (fingerprint)' in error_msg
        assert 'ANLA (status)' in error_msg
        assert 'BSEG' not in error_msg

    @patch('amue.tasks.import_dag.validate_tables.logger')
    def test_validate_tables_success(self, mock_logger):
        """Pas d'exception quand tout est OK"""
        from amue.tasks.import_dag.validate_tables import validate_tables

        results = [
            {
                'table_name': 'CSKS',
                'status': 'success',
                'phase': 'complete',
                'error': None
            }
        ]

        validated = validate_tables.function(results)
        assert len(validated) == 1
        assert validated[0]['table_name'] == 'CSKS'
