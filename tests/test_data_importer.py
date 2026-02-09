"""
Tests unitaires pour le data_importer et ses sous-modules
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestDataStreamer:
    """Tests pour AMUEDataStreamer"""

    def test_format_date_for_query_valid(self):
        """Test du formatage de date valide"""
        from amue.operators.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        # Date ISO valide
        assert streamer._format_date_for_query('2024-01-15') == '20240115'
        assert streamer._format_date_for_query('2024-01-15T10:30:00Z') == '20240115'

    def test_format_date_for_query_invalid(self):
        """Test du formatage de date invalide (fallback)"""
        from amue.operators.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        # Date invalide -> fallback (replace '-' puis prendre les 8 premiers chars)
        assert streamer._format_date_for_query('invalid-date') == 'invalidd'
        assert streamer._format_date_for_query('20240115') == '20240115'

    def test_build_query_params_full(self):
        """Test construction params pour import full"""
        from amue.operators.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        import_config = {'import_type': 'full'}
        params = streamer._build_query_params('CSKS', import_config)

        assert params['nom'] == 'CSKS'
        assert params['f'] == 'json'
        assert 'q' not in params

    def test_build_query_params_differential(self):
        """Test construction params pour import differentiel avec plage (>=)"""
        from amue.operators.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        import_config = {
            'import_type': 'differential',
            'delta': 'AEDAT',
            'last_import': '2024-01-15'
        }
        params = streamer._build_query_params('CSKS', import_config)

        assert params['nom'] == 'CSKS'
        assert 'q' in params
        assert 'AEDAT' in params['q']
        assert '20240115' in params['q']
        # Vérifie que c'est bien >= (plage) et pas = (égalité)
        assert ">=" in params['q']


class TestBatchInserter:
    """Tests pour AMUEBatchInserter"""

    @patch('amue.operators.batch_inserter.sql')
    def test_build_insert_sql_simple(self, mock_sql):
        """Test construction requete INSERT simple"""
        from amue.operators.batch_inserter import AMUEBatchInserter

        inserter = AMUEBatchInserter()

        # Configure le mock pour retourner une chaine SQL
        mock_composed = MagicMock()
        mock_composed.as_string.return_value = 'INSERT INTO "test_table" ("col1", "col2") VALUES (%s, %s)'
        mock_sql.SQL.return_value.format.return_value = mock_composed
        mock_sql.Identifier.side_effect = lambda x: f'"{x}"'
        mock_sql.Placeholder.return_value = '%s'

        mock_conn = MagicMock()

        sql_query = inserter.build_insert_sql(
            'test_table',
            ['col1', 'col2'],
            [],
            use_upsert=False,
            conn=mock_conn
        )

        assert 'INSERT INTO' in sql_query

    @patch('amue.operators.batch_inserter.sql')
    def test_build_insert_sql_upsert(self, mock_sql):
        """Test construction requete UPSERT"""
        from amue.operators.batch_inserter import AMUEBatchInserter

        inserter = AMUEBatchInserter()

        # Configure le mock pour retourner une chaine SQL avec UPSERT
        mock_composed = MagicMock()
        mock_composed.as_string.return_value = (
            'INSERT INTO "test_table" ("pk_col", "data_col") VALUES (%s, %s) '
            'ON CONFLICT ("pk_col") DO UPDATE SET "data_col" = EXCLUDED."data_col"'
        )
        mock_sql.SQL.return_value.format.return_value = mock_composed
        mock_sql.Identifier.side_effect = lambda x: f'"{x}"'
        mock_sql.Placeholder.return_value = '%s'

        mock_conn = MagicMock()

        sql_query = inserter.build_insert_sql(
            'test_table',
            ['pk_col', 'data_col'],
            ['pk_col'],
            use_upsert=True,
            conn=mock_conn
        )

        assert 'INSERT INTO' in sql_query
        assert 'ON CONFLICT' in sql_query
        assert 'DO UPDATE SET' in sql_query


class TestDuplicateDetector:
    """Tests pour DuplicateDetector"""

    def test_detect_duplicates_empty_batch(self):
        """Detection sur batch vide"""
        from amue.operators.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()
        result = detector.detect_duplicates_in_batch([], ['col1', 'col2'], ['col1'])

        assert result == {}

    def test_detect_duplicates_no_duplicates(self):
        """Pas de doublons"""
        from amue.operators.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()
        batch = [
            ('pk1', 'data1'),
            ('pk2', 'data2'),
            ('pk3', 'data3'),
        ]

        result = detector.detect_duplicates_in_batch(batch, ['pk_col', 'data_col'], ['pk_col'])

        assert result == {}

    def test_detect_duplicates_with_duplicates(self):
        """Detection de doublons"""
        from amue.operators.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()
        batch = [
            ('pk1', 'data1'),
            ('pk2', 'data2'),
            ('pk1', 'data3'),  # Doublon sur pk1
        ]

        result = detector.detect_duplicates_in_batch(batch, ['pk_col', 'data_col'], ['pk_col'])

        assert len(result) == 1
        # La cle doit contenir 'pk1'
        assert any('pk1' in key for key in result.keys())

    def test_find_duplicates_for_pk(self):
        """Recherche de doublons pour une PK specifique"""
        from amue.operators.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()
        batch = [
            ('pk1', 'data1'),
            ('pk2', 'data2'),
            ('pk1', 'data3'),
        ]

        result = detector.find_duplicates_for_pk(
            batch,
            ['pk_col', 'data_col'],
            ['pk_col'],
            {'pk_col': 'pk1'}
        )

        assert len(result) == 2

    def test_extract_pk_from_error(self):
        """Extraction de PK depuis un message d'erreur PostgreSQL"""
        from amue.operators.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()

        error_msg = "DETAIL: La cle « (bukrs, kostl)=(1000, TEST01) » existe deja."

        result = detector.extract_pk_from_error(error_msg, ['bukrs', 'kostl'])

        assert result is not None
        assert 'bukrs' in result
        assert 'kostl' in result


class TestDataImporterIntegration:
    """Tests d'integration pour AMUEDataImporter"""

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_importer_initialization(self, mock_postgres, mock_varmgr):
        """Test de l'initialisation de l'importer"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_table': 'https://api.example.com/$univ/table',
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }.get(key, default)

        from amue.operators.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        assert importer.batch_size == 5000
        assert importer.max_retries == 3
        assert 'ul' in importer.endpoint
        assert importer.streamer is not None
        assert importer.inserter is not None

    @patch('amue.operators.data_importer.VarMgr')
    @patch('amue.operators.data_importer.PostgresHook')
    def test_importer_missing_universite(self, mock_postgres, mock_varmgr):
        """Test avec variable universite manquante"""
        from airflow.exceptions import AirflowException
        from amue.operators.data_importer import AMUEDataImporter

        mock_varmgr.get.side_effect = KeyError('universite')

        mock_api_hook = MagicMock()

        with pytest.raises(AirflowException):
            AMUEDataImporter(mock_api_hook)
