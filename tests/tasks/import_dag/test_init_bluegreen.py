"""Tests pour la task init_bluegreen."""
from unittest.mock import MagicMock, patch


class TestInitBluegreen:
    """Tests pour la task d'initialisation blue/green."""

    def _make_manager(self, target='splus_green', active='splus_blue', needs_sync=False):
        """Helper : construit un BlueGreenManager mocké."""
        mgr = MagicMock()
        mgr.get_target_schema.return_value = target
        mgr.get_active_schema.return_value = active
        mgr.needs_sync.return_value = needs_sync
        return mgr

    @patch('amue.tasks.import_dag.init_bluegreen.get_current_context')
    @patch('amue.tasks.import_dag.init_bluegreen.BlueGreenManager')
    def test_init_bluegreen_returns_expected_keys(self, mock_bgm_cls, mock_get_ctx):
        """La task retourne un dict avec les 4 clés attendues."""
        from amue.tasks.import_dag.init_bluegreen import init_bluegreen

        mock_bgm_cls.return_value = self._make_manager()
        mock_get_ctx.return_value = {'dag_run': MagicMock(run_id='run_abc')}

        result = init_bluegreen.function()

        assert set(result.keys()) == {'enabled', 'target_schema', 'active_schema', 'needs_sync'}
        assert result['enabled'] is True
        assert result['target_schema'] == 'splus_green'
        assert result['active_schema'] == 'splus_blue'
        assert result['needs_sync'] is False

    @patch('amue.tasks.import_dag.init_bluegreen.get_current_context')
    @patch('amue.tasks.import_dag.init_bluegreen.BlueGreenManager')
    def test_init_bluegreen_fallback_run_id_on_exception(self, mock_bgm_cls, mock_get_ctx):
        """Quand get_current_context lève une exception, la task ne plante pas."""
        from amue.tasks.import_dag.init_bluegreen import init_bluegreen

        mock_bgm_cls.return_value = self._make_manager()
        mock_get_ctx.side_effect = RuntimeError("contexte indisponible")

        result = init_bluegreen.function()

        assert set(result.keys()) == {'enabled', 'target_schema', 'active_schema', 'needs_sync'}
        assert result['enabled'] is True
