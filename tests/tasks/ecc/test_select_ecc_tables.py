"""Tests unitaires pour select_ecc_tables."""
from unittest.mock import MagicMock, patch


def _run_select(rows, active_schema='splus_blue'):
    """Helper: exécute select_ecc_tables avec mocks actifs."""
    from ecc.tasks.import_dag.select_tables import select_ecc_tables

    pg_hook = MagicMock()
    pg_hook.get_records.return_value = rows

    with patch('ecc.tasks.import_dag.select_tables.BlueGreenManager') as MockBG, \
         patch('ecc.tasks.import_dag.select_tables.create_postgres_hook', return_value=pg_hook):
        MockBG.return_value.get_active_schema.return_value = active_schema
        return select_ecc_tables.function()


class TestSelectEccTables:

    def test_returns_one_row_per_table(self):
        rows = [
            ('CSKS', 'SELECT * FROM csks', 'KOKRS,KOSTL'),
            ('LFA1', 'SELECT * FROM lfa1', 'LIFNR'),
        ]
        result = _run_select(rows, active_schema='splus_blue')
        assert len(result) == 2

    def test_table_config_structure(self):
        rows = [('CSKS', 'SELECT * FROM csks', 'KOKRS,KOSTL')]
        result = _run_select(rows, active_schema='splus_green')
        entry = result[0]
        assert entry['table_name'] == 'CSKS'
        assert entry['ecc_query'] == 'SELECT * FROM csks'
        assert entry['primary_keys'] == ['KOKRS', 'KOSTL']
        assert entry['target_schema'] == 'splus_green'
        assert 'source' in entry
        assert 'protected_source' in entry

    def test_primary_keys_parsed_as_list(self):
        rows = [('T', 'SELECT 1', 'PK1, PK2 , PK3')]
        result = _run_select(rows)
        assert result[0]['primary_keys'] == ['PK1', 'PK2', 'PK3']

    def test_empty_rows_returns_empty_list(self):
        result = _run_select(rows=[])
        assert result == []

    def test_uses_active_schema(self):
        rows = [('T', 'SELECT 1', 'PK')]
        result = _run_select(rows, active_schema='splus_blue')
        assert result[0]['target_schema'] == 'splus_blue'
