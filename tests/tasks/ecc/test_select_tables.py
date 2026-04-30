"""Tests unitaires pour select_tables."""
from unittest.mock import MagicMock, patch


def _run_select(rows, active_schema='splus_blue', inactive_canonical='splus_green',
                inactive_exists=True, inactive_offline_exists=False):
    """Helper: exécute select_tables avec mocks actifs."""
    from ecc.tasks.import_dag.select_tables import select_tables

    pg_hook = MagicMock()
    pg_hook.get_records.return_value = rows

    with patch('ecc.tasks.import_dag.select_tables.BlueGreenManager') as MockBG, \
         patch('ecc.tasks.import_dag.select_tables.create_postgres_hook', return_value=pg_hook):
        instance = MockBG.return_value
        instance.get_active_schema.return_value = active_schema
        instance.get_target_schema.return_value = inactive_canonical
        MockBG.OFFLINE_SUFFIX = '_offline'

        def schema_exists(name):
            if name == inactive_canonical:
                return inactive_exists
            if name == inactive_canonical + '_offline':
                return inactive_offline_exists
            return False

        instance.schema_exists.side_effect = schema_exists
        return select_tables.function()


class TestSelectEccTables:

    def test_returns_one_row_per_table(self):
        rows = [
            ('CSKS', 'SELECT * FROM csks', 'KOKRS,KOSTL'),
            ('LFA1', 'SELECT * FROM lfa1', 'LIFNR'),
        ]
        result = _run_select(rows, active_schema='splus_blue')
        # 2 tables × 1 schéma (inactif uniquement) = 2 entrées
        assert len(result) == 2

    def test_table_config_structure(self):
        rows = [('CSKS', 'SELECT * FROM csks', 'KOKRS,KOSTL')]
        result = _run_select(rows, active_schema='splus_blue', inactive_canonical='splus_green')
        # Inactif uniquement
        schemas = {e['target_schema'] for e in result}
        assert 'splus_blue' not in schemas
        assert 'splus_green' in schemas
        entry = result[0]
        assert entry['table_name'] == 'CSKS'
        assert entry['ecc_query'] == 'SELECT * FROM csks'
        assert entry['primary_keys'] == ['KOKRS', 'KOSTL']
        assert 'source' in entry
        assert 'protected_source' in entry

    def test_primary_keys_parsed_as_list(self):
        rows = [('T', 'SELECT 1', 'PK1, PK2 , PK3')]
        result = _run_select(rows)
        assert result[0]['primary_keys'] == ['PK1', 'PK2', 'PK3']

    def test_empty_rows_returns_empty_list(self):
        result = _run_select(rows=[])
        assert result == []

    def test_uses_inactive_schema_only(self):
        rows = [('T', 'SELECT 1', 'PK')]
        result = _run_select(rows, active_schema='splus_blue', inactive_canonical='splus_green')
        schemas = [e['target_schema'] for e in result]
        assert 'splus_blue' not in schemas
        assert 'splus_green' in schemas
        assert len(result) == 1

    def test_inactive_schema_offline(self):
        rows = [('T', 'SELECT 1', 'PK')]
        result = _run_select(rows, active_schema='splus_blue', inactive_canonical='splus_green',
                             inactive_exists=False, inactive_offline_exists=True)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_blue' not in schemas
        assert 'splus_green_offline' in schemas
        assert len(result) == 1

    def test_inactive_schema_missing(self):
        rows = [('T', 'SELECT 1', 'PK'), ('T2', 'SELECT 2', 'PK2')]
        result = _run_select(rows, active_schema='splus_blue', inactive_canonical='splus_green',
                             inactive_exists=False, inactive_offline_exists=False)
        # Aucun schéma inactif → N entrées uniquement
        assert len(result) == 2
        assert all(e['target_schema'] == 'splus_blue' for e in result)
