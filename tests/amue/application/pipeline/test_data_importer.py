"""
Tests unitaires pour le data_importer et ses sous-modules
"""
import queue
import threading
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call


class TestDataStreamer:
    """Tests pour AMUEDataStreamer"""

    def test_format_date_for_query_valid(self):
        """Test du formatage de date valide"""
        from amue.infrastructure.api.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        # Date ISO valide
        assert streamer._format_date_for_query('2024-01-15') == '20240115'
        assert streamer._format_date_for_query('2024-01-15T10:30:00Z') == '20240115'

    def test_format_date_for_query_invalid(self):
        """Test du formatage de date invalide (fallback)"""
        from amue.infrastructure.api.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        # Date invalide -> fallback (replace '-' puis prendre les 8 premiers chars)
        assert streamer._format_date_for_query('invalid-date') == 'invalidd'
        assert streamer._format_date_for_query('20240115') == '20240115'

    def test_build_query_params_full(self):
        """Test construction params pour import full"""
        from amue.infrastructure.api.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        import_config = {'import_type': 'full'}
        params = streamer._build_query_params('CSKS', import_config)

        assert params['nom'] == 'CSKS'
        assert params['f'] == 'json'
        assert 'q' not in params

    def test_build_query_params_differential(self):
        """Test construction params pour import differentiel avec plage (>=)"""
        from amue.infrastructure.api.data_streamer import AMUEDataStreamer

        mock_api_hook = MagicMock()
        streamer = AMUEDataStreamer(mock_api_hook, 'https://api.example.com/table')

        import_config = {
            'import_type': 'delta',
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
    """Tests pour BatchUpserter"""

    @patch('common.application.batch_upserter.sql')
    def test_build_insert_sql_simple(self, mock_sql):
        """Test construction requete INSERT simple"""
        from common.application.batch_upserter import BatchUpserter

        inserter = BatchUpserter()

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

    @patch('common.application.batch_upserter.sql')
    def test_build_insert_sql_upsert(self, mock_sql):
        """Test construction requete UPSERT"""
        from common.application.batch_upserter import BatchUpserter

        inserter = BatchUpserter()

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
        from common.application.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()
        result = detector.detect_duplicates_in_batch([], ['col1', 'col2'], ['col1'])

        assert result == {}

    def test_detect_duplicates_no_duplicates(self):
        """Pas de doublons"""
        from common.application.duplicate_detector import DuplicateDetector

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
        from common.application.duplicate_detector import DuplicateDetector

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
        from common.application.duplicate_detector import DuplicateDetector

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
        from common.application.duplicate_detector import DuplicateDetector

        detector = DuplicateDetector()

        error_msg = "DETAIL: La cle « (bukrs, kostl)=(1000, TEST01) » existe deja."

        result = detector.extract_pk_from_error(error_msg, ['bukrs', 'kostl'])

        assert result is not None
        assert 'bukrs' in result
        assert 'kostl' in result


class TestDataImporterIntegration:
    """Tests d'integration pour AMUEDataImporter"""

    @patch('amue.application.pipeline.data_importer.get_data_streamer')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_importer_initialization(self, mock_varmgr, mock_get_streamer):
        """Test de l'initialisation de l'importer"""
        _vars = {
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
            'amue_import_parallel_workers': '1',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]
        mock_varmgr.get_int.side_effect = lambda key, default=None, min_value=None, max_value=None: int(
            mock_varmgr.get(key, default)
        )

        from amue.application.pipeline.data_importer import AMUEDataImporter

        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)

        assert importer.batch_size == 5000
        assert importer.max_retries == 3
        assert importer.streamer is not None
        assert importer.inserter is not None

    @patch('amue.application.pipeline.data_importer.get_data_streamer')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_importer_missing_universite(self, mock_varmgr, mock_get_streamer):
        """Test avec variable universite manquante — propagée depuis la factory"""
        from airflow.exceptions import AirflowException
        from amue.application.pipeline.data_importer import AMUEDataImporter

        mock_get_streamer.side_effect = AirflowException(
            "La variable 'universite' doit être définie"
        )

        mock_api_hook = MagicMock()

        with pytest.raises(AirflowException):
            AMUEDataImporter(mock_api_hook)


class TestParallelInsertion:
    """Tests pour l'insertion parallèle."""

    def _make_importer(self, mock_varmgr, parallel_workers=1):
        """Crée un importer configuré pour les tests."""
        _vars = {
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
            'amue_import_parallel_workers': str(parallel_workers),
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]
        mock_varmgr.get_int.side_effect = lambda key, default=None, min_value=None, max_value=None: int(
            mock_varmgr.get(key, default)
        )

        from amue.application.pipeline.data_importer import AMUEDataImporter
        mock_api_hook = MagicMock()
        return AMUEDataImporter(mock_api_hook)

    @patch('amue.application.pipeline.data_importer.get_data_streamer')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_single_worker_uses_sequential(self, mock_varmgr, mock_get_streamer):
        """parallel_workers=1 utilise le chemin séquentiel."""
        importer = self._make_importer(mock_varmgr, parallel_workers=1)
        assert importer.parallel_workers == 1

    @patch('amue.application.pipeline.data_importer.get_data_streamer')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_multi_worker_config(self, mock_varmgr, mock_get_streamer):
        """parallel_workers>1 est bien stocké."""
        importer = self._make_importer(mock_varmgr, parallel_workers=3)
        assert importer.parallel_workers == 3

    @patch('amue.application.pipeline.data_importer.get_data_streamer')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_parallel_workers_default_is_one(self, mock_varmgr, mock_get_streamer):
        """Par défaut, parallel_workers=1 (séquentiel)."""
        _vars = {
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': '5000',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]
        mock_varmgr.get_int.side_effect = lambda key, default=None, min_value=None, max_value=None: int(
            mock_varmgr.get(key, default)
        )

        from amue.application.pipeline.data_importer import AMUEDataImporter
        mock_api_hook = MagicMock()
        importer = AMUEDataImporter(mock_api_hook)
        assert importer.parallel_workers == 1


class TestQueueBasedInsertion:
    """Tests pour le pipeline producteur/consommateur avec queue."""

    def _make_importer(self, mock_varmgr, parallel_workers=1, batch_size=5):
        """Cree un importer configure pour les tests."""
        _vars = {
            'amue_api_max_retries': '3',
            'amue_api_retry_delay_seconds': '30',
            'amue_import_batch_size': str(batch_size),
            'amue_import_parallel_workers': str(parallel_workers),
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]
        mock_varmgr.get_int.side_effect = lambda key, default=None, min_value=None, max_value=None: int(
            mock_varmgr.get(key, default)
        )

        from amue.application.pipeline.data_importer import AMUEDataImporter
        mock_api_hook = MagicMock()
        with patch('amue.application.pipeline.data_importer.get_data_streamer'):
            importer = AMUEDataImporter(mock_api_hook)
        return importer

    def _make_rows(self, count, columns):
        """Genere des rows dict pour le streamer mock."""
        return [
            {col: f"val_{col}_{i}" for col in columns}
            for i in range(count)
        ]

    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_single_worker_pipeline(self, mock_varmgr):
        """12 rows, batch_size=5, workers=1 -> 3 batches (5+5+2), totaux corrects."""
        importer = self._make_importer(mock_varmgr, parallel_workers=1, batch_size=5)

        columns = ['col_a', 'col_b']
        rows = self._make_rows(12, columns)

        # Mock streamer
        importer.streamer.stream_data = MagicMock(return_value=iter(rows))

        # Mock inserter — retourne rows_inserted/rows_updated en fonction de la taille du batch
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        importer.inserter.get_connection = MagicMock(return_value=mock_conn)
        importer.inserter.build_insert_sql_for_values = MagicMock(return_value='INSERT SQL')

        def fake_execute_batch(cursor, conn, sql, batch_data, *args, **kwargs):
            n = len(batch_data)
            return {'duration_seconds': 0.1, 'rows_inserted': n, 'rows_updated': 0, 'rows_affected': n}

        importer.inserter.execute_batch = MagicMock(side_effect=fake_execute_batch)

        total_inserted, total_updated, total_fetched, batch_metrics = importer._stream_and_insert(
            'test_table', columns + ['_source', '_imported_at'],
            ['col_a'], {'import_type': 'full'}, True, 'corr-1'
        )

        assert total_fetched == 12
        assert total_inserted == 12
        assert total_updated == 0
        assert len(batch_metrics) == 3  # 5+5+2
        assert importer.inserter.execute_batch.call_count == 3

        # Verifie la taille des batches
        call_args = importer.inserter.execute_batch.call_args_list
        assert len(call_args[0][0][3]) == 5  # batch 1: 5 rows
        assert len(call_args[1][0][3]) == 5  # batch 2: 5 rows
        assert len(call_args[2][0][3]) == 2  # batch 3: 2 rows

    @patch('amue.application.pipeline.data_importer.create_postgres_hook')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_multi_worker_pipeline(self, mock_varmgr, mock_create_hook):
        """workers=2, 12 rows, batch_size=5 -> tous les batches traites."""
        importer = self._make_importer(mock_varmgr, parallel_workers=2, batch_size=5)

        columns = ['col_a', 'col_b']
        rows = self._make_rows(12, columns)

        importer.streamer.stream_data = MagicMock(return_value=iter(rows))

        # Mock pour les workers crees dynamiquement
        mock_worker_hook = MagicMock()
        mock_create_hook.return_value = mock_worker_hook

        batches_received = []
        lock = threading.Lock()

        def fake_execute_batch(cursor, conn, sql, batch_data, *args, **kwargs):
            n = len(batch_data)
            with lock:
                batches_received.append(n)
            return {'duration_seconds': 0.01, 'rows_inserted': n, 'rows_updated': 0, 'rows_affected': n}

        with patch('amue.application.pipeline.data_importer.BatchUpserter') as MockInserter:
            mock_worker = MagicMock()
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_worker.get_connection.return_value = mock_conn
            mock_worker.build_insert_sql_for_values.return_value = 'INSERT SQL'
            mock_worker.execute_batch = MagicMock(side_effect=fake_execute_batch)
            MockInserter.return_value = mock_worker

            total_inserted, total_updated, total_fetched, batch_metrics = importer._stream_and_insert(
                'test_table', columns + ['_source', '_imported_at'],
                ['col_a'], {'import_type': 'full'}, True, 'corr-2'
            )

        assert total_fetched == 12
        assert total_inserted == 12
        assert sum(batches_received) == 12

    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_consumer_error_propagates(self, mock_varmgr):
        """execute_batch raise au 2e batch -> AMUEBatchError propagee via f.result()."""
        from amue.domain.exceptions import AMUEBatchError, AMUEDatabaseError
        importer = self._make_importer(mock_varmgr, parallel_workers=1, batch_size=5)

        columns = ['col_a']
        rows = self._make_rows(12, columns)

        importer.streamer.stream_data = MagicMock(return_value=iter(rows))

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        importer.inserter.get_connection = MagicMock(return_value=mock_conn)
        importer.inserter.build_insert_sql_for_values = MagicMock(return_value='INSERT SQL')

        call_count = 0

        def fail_on_second(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise AMUEBatchError("Erreur batch 2", batch_num=2)
            return {'duration_seconds': 0.01}

        importer.inserter.execute_batch = MagicMock(side_effect=fail_on_second)

        with pytest.raises((AMUEBatchError, AMUEDatabaseError)):
            importer._stream_and_insert(
                'test_table', ['col_a', '_source', '_imported_at'],
                ['col_a'], {'import_type': 'full'}, True, 'corr-3'
            )

    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_producer_error_propagates(self, mock_varmgr):
        """stream_data raise apres quelques rows -> exception propagee."""
        from amue.domain.exceptions import AMUEDatabaseError
        importer = self._make_importer(mock_varmgr, parallel_workers=1, batch_size=5)

        def failing_stream(*args):
            yield {'col_a': 'v1'}
            yield {'col_a': 'v2'}
            raise RuntimeError("API connection lost")

        importer.streamer.stream_data = MagicMock(side_effect=failing_stream)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        importer.inserter.get_connection = MagicMock(return_value=mock_conn)
        importer.inserter.build_insert_sql_for_values = MagicMock(return_value='INSERT SQL')
        importer.inserter.execute_batch = MagicMock(return_value={'duration_seconds': 0.01, 'rows_inserted': 1, 'rows_updated': 0, 'rows_affected': 1})

        with pytest.raises(AMUEDatabaseError):
            importer._stream_and_insert(
                'test_table', ['col_a', '_source', '_imported_at'],
                ['col_a'], {'import_type': 'full'}, True, 'corr-4'
            )

    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_empty_stream(self, mock_varmgr):
        """0 rows -> retourne (0, 0, [])."""
        importer = self._make_importer(mock_varmgr, parallel_workers=1, batch_size=5)

        importer.streamer.stream_data = MagicMock(return_value=iter([]))

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        importer.inserter.get_connection = MagicMock(return_value=mock_conn)
        importer.inserter.build_insert_sql_for_values = MagicMock(return_value='INSERT SQL')

        total_inserted, total_updated, total_fetched, batch_metrics = importer._stream_and_insert(
            'test_table', ['col_a', '_source', '_imported_at'],
            ['col_a'], {'import_type': 'full'}, True, 'corr-5'
        )

        assert total_inserted == 0
        assert total_updated == 0
        assert total_fetched == 0
        assert batch_metrics == []

    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_meta_columns_appended(self, mock_varmgr):
        """Verifie _source et _imported_at dans chaque tuple."""
        importer = self._make_importer(mock_varmgr, parallel_workers=1, batch_size=10)

        columns = ['col_a']
        rows = [{'col_a': 'val1'}, {'col_a': 'val2'}]

        importer.streamer.stream_data = MagicMock(return_value=iter(rows))

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        importer.inserter.get_connection = MagicMock(return_value=mock_conn)
        importer.inserter.build_insert_sql_for_values = MagicMock(return_value='INSERT SQL')
        importer.inserter.execute_batch = MagicMock(return_value={'duration_seconds': 0.01, 'rows_inserted': 1, 'rows_updated': 0, 'rows_affected': 1})

        importer._stream_and_insert(
            'test_table', ['col_a', '_source', '_imported_at'],
            ['col_a'], {'import_type': 'full'}, True, 'corr-6'
        )

        # Verifie le batch passe a execute_batch
        batch_arg = importer.inserter.execute_batch.call_args[0][3]
        assert len(batch_arg) == 2
        for record in batch_arg:
            assert len(record) == 3  # col_a + _source + _imported_at
            assert record[1] == 'sifac_plus'  # default_source
            assert isinstance(record[2], datetime)  # _imported_at

    @patch('amue.application.pipeline.data_importer.create_postgres_hook')
    @patch('amue.application.pipeline.data_importer.VarMgr')
    def test_multi_worker_connections_closed(self, mock_varmgr, mock_create_hook):
        """workers=3, verifie close_connection() appele 3 fois."""
        importer = self._make_importer(mock_varmgr, parallel_workers=3, batch_size=5)

        columns = ['col_a']
        rows = self._make_rows(3, columns)

        importer.streamer.stream_data = MagicMock(return_value=iter(rows))

        mock_workers = []

        with patch('amue.application.pipeline.data_importer.BatchUpserter') as MockInserter:
            def create_mock_worker(*args, **kwargs):
                mock_worker = MagicMock()
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_conn.cursor.return_value = mock_cursor
                mock_worker.get_connection.return_value = mock_conn
                mock_worker.build_insert_sql_for_values.return_value = 'INSERT SQL'
                mock_worker.execute_batch.return_value = {'duration_seconds': 0.01, 'rows_inserted': 1, 'rows_updated': 0, 'rows_affected': 1}
                mock_workers.append(mock_worker)
                return mock_worker

            MockInserter.side_effect = create_mock_worker

            importer._stream_and_insert(
                'test_table', ['col_a', '_source', '_imported_at'],
                ['col_a'], {'import_type': 'full'}, True, 'corr-8'
            )

        # 3 workers crees, chacun doit avoir close_connection appele
        assert len(mock_workers) == 3
        for w in mock_workers:
            w.close_connection.assert_called_once()
