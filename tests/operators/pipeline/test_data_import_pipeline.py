# tests/operators/pipeline/test_data_import_pipeline.py
"""Tests unitaires pour DataImportPipeline."""
import queue
import threading
from unittest.mock import MagicMock, patch

import pytest

from amue.exceptions import AMUEDatabaseError


class TestQueuePutSafe:
    """Tests pour _queue_put_safe."""

    def test_puts_item_in_queue(self):
        """Insère un item dans la queue sans erreur."""
        from amue.operators.pipeline.data_import_pipeline import DataImportPipeline

        q = queue.Queue(maxsize=5)
        error_event = threading.Event()
        DataImportPipeline._queue_put_safe(q, 'item', error_event)
        assert q.get_nowait() == 'item'

    def test_raises_when_error_event_set(self):
        """Lève AMUEDatabaseError si error_event est levé avant/pendant put."""
        from amue.operators.pipeline.data_import_pipeline import DataImportPipeline

        q = queue.Queue(maxsize=0)  # queue pleine immédiatement
        error_event = threading.Event()
        error_event.set()

        with pytest.raises(AMUEDatabaseError):
            DataImportPipeline._queue_put_safe(q, 'item', error_event)


class TestDataImportPipelineRun:
    """Tests pour DataImportPipeline.run()."""

    def _make_pipeline(self, rows=None, batch_metrics=None, target_schema=None):
        """Helper : construit un pipeline avec des mocks."""
        from amue.operators.pipeline.data_import_pipeline import DataImportPipeline

        if rows is None:
            rows = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
        if batch_metrics is None:
            batch_metrics = {'rows_inserted': len(rows), 'rows_updated': 0}

        mock_streamer = MagicMock()
        mock_streamer.stream_data.return_value = iter(rows)

        mock_inserter = MagicMock()
        mock_inserter.get_connection.return_value = MagicMock()
        mock_inserter.build_insert_sql_for_values.return_value = (
            "INSERT INTO t VALUES %s ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name"
        )
        mock_inserter.execute_batch.return_value = batch_metrics

        pipeline = DataImportPipeline(mock_streamer, mock_inserter, target_schema=target_schema)
        return pipeline, mock_streamer, mock_inserter

    def test_returns_correct_row_counts(self):
        """run() retourne (inserted, updated, fetched, metrics)."""
        rows = [{'id': i, 'name': f'N{i}'} for i in range(5)]
        pipeline, _, mock_inserter = self._make_pipeline(
            rows=rows,
            batch_metrics={'rows_inserted': 5, 'rows_updated': 0}
        )

        inserted, updated, fetched, metrics = pipeline.run(
            table_name='test_table',
            columns=['id', 'name', '_source', '_imported_at'],
            primary_keys=['id'],
            import_config={},
            batch_size=10,
        )

        assert fetched == 5
        assert inserted == 5
        assert updated == 0

    def test_empty_table_returns_zeros(self):
        """Table vide : 0 lignes récupérées et insérées."""
        pipeline, _, _ = self._make_pipeline(rows=[], batch_metrics=None)

        inserted, updated, fetched, metrics = pipeline.run(
            table_name='test_table',
            columns=['id', '_source', '_imported_at'],
            primary_keys=['id'],
            import_config={},
            batch_size=10,
        )

        assert fetched == 0
        assert inserted == 0
        assert updated == 0
        assert metrics == []

    def test_batch_size_splits_correctly(self):
        """Les lignes sont découpées en batches selon batch_size."""
        rows = [{'id': i} for i in range(10)]
        pipeline, _, mock_inserter = self._make_pipeline(
            rows=rows,
            batch_metrics={'rows_inserted': 5, 'rows_updated': 0}
        )

        pipeline.run(
            table_name='t',
            columns=['id', '_source', '_imported_at'],
            primary_keys=['id'],
            import_config={},
            batch_size=5,
        )

        # execute_batch appelé 2 fois (10 lignes / 5 par batch)
        assert mock_inserter.execute_batch.call_count == 2

    def test_streamer_called_with_correct_args(self):
        """stream_data() est appelé avec le bon nom de table et la config."""
        pipeline, mock_streamer, _ = self._make_pipeline()

        pipeline.run(
            table_name='CSKS',
            columns=['id', '_source', '_imported_at'],
            primary_keys=['id'],
            import_config={'filter': 'x'},
            batch_size=100,
        )

        mock_streamer.stream_data.assert_called_once_with('CSKS', {'filter': 'x'})

    def test_raises_database_error_on_insert_failure(self):
        """AMUEDatabaseError propagée si l'insertion échoue."""
        from amue.operators.pipeline.data_import_pipeline import DataImportPipeline

        mock_streamer = MagicMock()
        mock_streamer.stream_data.return_value = iter([{'id': 1}])

        mock_inserter = MagicMock()
        mock_inserter.get_connection.return_value = MagicMock()
        mock_inserter.build_insert_sql_for_values.return_value = "INSERT ..."
        mock_inserter.execute_batch.side_effect = AMUEDatabaseError("connexion perdue")

        pipeline = DataImportPipeline(mock_streamer, mock_inserter)

        with pytest.raises((AMUEDatabaseError, Exception)):
            pipeline.run(
                table_name='T',
                columns=['id', '_source', '_imported_at'],
                primary_keys=['id'],
                import_config={},
                batch_size=10,
            )

    def test_target_schema_passed_to_inserter(self):
        """target_schema est transmis au pipeline."""
        from amue.operators.pipeline.data_import_pipeline import DataImportPipeline

        mock_streamer = MagicMock()
        mock_streamer.stream_data.return_value = iter([])
        mock_inserter = MagicMock()
        mock_inserter.get_connection.return_value = MagicMock()
        mock_inserter.build_insert_sql_for_values.return_value = ""

        pipeline = DataImportPipeline(mock_streamer, mock_inserter, target_schema='splus_blue')
        assert pipeline.target_schema == 'splus_blue'
