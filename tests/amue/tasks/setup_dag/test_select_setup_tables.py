"""Tests unitaires pour select_setup_tables."""
from unittest.mock import MagicMock, patch


def _run_select(active_canonical='splus_green', target_canonical='splus_blue',
                active_exists=True, active_offline_exists=False,
                inactive_exists=True, inactive_offline_exists=False,
                conf=None, tables=None):
    """Helper : exécute select_setup_tables avec mocks."""
    from amue.tasks.setup_dag.select_setup_tables import select_setup_tables

    if tables is None:
        tables = [
            {'name': 'CSKS', 'enable': True},
            {'name': 'LFA1', 'enable': True},
        ]

    def schema_exists(name):
        if name == active_canonical:
            return active_exists
        if name == active_canonical + '_offline':
            return active_offline_exists
        if name == target_canonical:
            return inactive_exists
        if name == target_canonical + '_offline':
            return inactive_offline_exists
        return False

    with patch('amue.tasks.setup_dag.select_setup_tables.BlueGreenManager') as MockBG, \
         patch('amue.tasks.setup_dag.select_setup_tables.TableConfigManager') as MockTCM, \
         patch('amue.tasks.setup_dag.select_setup_tables._read_dag_run_conf', return_value=conf or {}):
        instance = MockBG.return_value
        instance.get_active_schema.return_value = active_canonical
        instance.get_target_schema.return_value = target_canonical
        instance.schema_exists.side_effect = schema_exists
        MockBG.OFFLINE_SUFFIX = '_offline'
        MockTCM.return_value.get_tables_config.return_value = tables
        return select_setup_tables.function()


class TestSelectSetupTablesStandalone:

    def test_active_exists_canonical(self):
        """Schéma actif canonique → utilisé tel quel."""
        result = _run_select(active_canonical='splus_green', active_exists=True,
                             inactive_exists=True)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_green' in schemas
        assert 'splus_blue' in schemas
        assert len(result) == 4  # 2 tables × 2 schémas

    def test_active_is_offline(self):
        """Schéma actif canonique absent → fallback sur _offline."""
        result = _run_select(active_canonical='splus_green',
                             active_exists=False, active_offline_exists=True,
                             inactive_exists=True)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_green_offline' in schemas
        assert 'splus_blue' in schemas

    def test_inactive_is_offline(self):
        """Schéma inactif canonique absent → fallback sur _offline."""
        result = _run_select(active_exists=True,
                             inactive_exists=False, inactive_offline_exists=True)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_green' in schemas
        assert 'splus_blue_offline' in schemas

    def test_both_offline(self):
        """Les deux schémas sont en version _offline."""
        result = _run_select(active_canonical='splus_green',
                             active_exists=False, active_offline_exists=True,
                             inactive_exists=False, inactive_offline_exists=True)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_green_offline' in schemas
        assert 'splus_blue_offline' in schemas

    def test_inactive_missing_fallback_canonical(self):
        """Schéma inactif absent (ni canonique ni _offline) → fallback sur le nom canonique."""
        result = _run_select(active_exists=True,
                             inactive_exists=False, inactive_offline_exists=False)
        schemas = {e['target_schema'] for e in result}
        assert 'splus_blue' in schemas
        assert len(result) == 4  # 2 tables × 2 schémas

    def test_conf_target_schema_bypasses_checks(self):
        """En mode déclenché (conf présent), target_schema est utilisé directement."""
        result = _run_select(conf={'target_schema': 'splus_blue'})
        assert all(e['target_schema'] == 'splus_blue' for e in result)

    def test_only_enabled_tables(self):
        """Seules les tables enabled=True sont incluses."""
        tables = [
            {'name': 'A', 'enable': True},
            {'name': 'B', 'enable': False},
            {'name': 'C', 'enable': True},
        ]
        result = _run_select(tables=tables, inactive_exists=False, inactive_offline_exists=False)
        names = {e['name'] for e in result}
        assert 'A' in names
        assert 'C' in names
        assert 'B' not in names
