"""Tests unitaires pour la task save_metadata."""
from unittest.mock import MagicMock, patch


class TestSaveEccMetadata:
    """Tests pour la task de sauvegarde des métadonnées ECC."""

    def _make_import_result(self, table_name='lfa1', status='success', rows_fetched=100):
        return {
            'table_name': table_name,
            'rows_fetched': rows_fetched,
            'rows_inserted': rows_fetched,
            'rows_updated': 0,
            'status': status,
            'target_schema': 'splus_blue',
        }

    def test_returns_expected_keys(self):
        """Le résultat contient table_name, import_timestamp, rows_imported."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function(self._make_import_result())

        assert 'table_name' in result
        assert 'import_timestamp' in result
        assert 'rows_imported' in result

    def test_table_name_preserved(self):
        """Le nom de table est préservé."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function(self._make_import_result('lfa1'))

        assert result['table_name'] == 'lfa1'

    def test_rows_imported_equals_rows_fetched(self):
        """rows_imported correspond à rows_fetched de l'import."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function(self._make_import_result(rows_fetched=250))

        assert result['rows_imported'] == 250

    def test_import_timestamp_is_iso_string(self):
        """import_timestamp est une chaîne ISO 8601."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function(self._make_import_result())

        ts = result['import_timestamp']
        assert isinstance(ts, str)
        assert 'T' in ts  # Format ISO 8601

    def test_handles_error_status(self):
        """Fonctionne avec un import en erreur (ne lève pas d'exception)."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function(self._make_import_result(status='error', rows_fetched=0))

        assert result['table_name'] == 'lfa1'
        assert result['rows_imported'] == 0

    def test_handles_missing_table_name(self):
        """Fonctionne si table_name est absent (utilise 'unknown')."""
        from ecc.tasks.import_dag.save_metadata import save_metadata

        result = save_metadata.function({'status': 'success', 'rows_fetched': 50})

        assert result['table_name'] == 'unknown'
        assert result['rows_imported'] == 50
