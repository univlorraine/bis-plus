"""
Tests unitaires pour le data_importer
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestDataImporterBackoff:
    """Tests pour le calcul du backoff exponentiel"""

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_calculate_backoff_first_attempt(self, mock_postgres, mock_varmgr):
        """Premier essai: délai de base"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/${univ}/table',
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        # Attempt 0 : 30 * 2^0 = 30 secondes
        assert importer._calculate_backoff(0) == 30

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_calculate_backoff_second_attempt(self, mock_postgres, mock_varmgr):
        """Deuxième essai: délai doublé"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/${univ}/table',
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        # Attempt 1 : 30 * 2^1 = 60 secondes
        assert importer._calculate_backoff(1) == 60

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_calculate_backoff_capped_at_max(self, mock_postgres, mock_varmgr):
        """Le backoff est plafonné à 5 minutes (300s)"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/${univ}/table',
            'amue_api_max_retries': '10',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        # Attempt 5 : 30 * 2^5 = 960 secondes, mais plafonné à 300
        assert importer._calculate_backoff(5) == 300

        # Même pour des tentatives élevées
        assert importer._calculate_backoff(10) == 300

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_format_date_for_query_valid(self, mock_postgres, mock_varmgr):
        """Test du formatage de date valide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/${univ}/table',
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        # Date ISO valide
        assert importer._format_date_for_query('2024-01-15') == '20240115'
        assert importer._format_date_for_query('2024-01-15T10:30:00Z') == '20240115'

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_format_date_for_query_invalid(self, mock_postgres, mock_varmgr):
        """Test du formatage de date invalide (fallback)"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/${univ}/table',
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        # Date invalide -> fallback (replace '-' puis prendre les 8 premiers chars)
        # 'invalid-date'.replace('-', '') = 'invaliddate' -> [:8] = 'invalidd'
        assert importer._format_date_for_query('invalid-date') == 'invalidd'
        assert importer._format_date_for_query('20240115') == '20240115'
