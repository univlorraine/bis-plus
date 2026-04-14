"""Tests pour la task switch_views."""
import pytest
from unittest.mock import MagicMock, patch


class TestSwitchViews:
    """Tests pour la task de bascule des vues blue/green."""

    def _make_manager(self, mark_completed_ok=True, mark_switch_ok=True, rename_ok=True):
        mgr = MagicMock()
        mgr.mark_import_completed.return_value = mark_completed_ok
        mgr.mark_switch_completed.return_value = mark_switch_ok
        mgr.rename_schema_to_offline.return_value = rename_ok
        return mgr

    def _make_switcher(self, switch_ok=True, verify_ok=True):
        sw = MagicMock()
        sw.switch_views_to_schema.return_value = switch_ok
        sw.verify_views_point_to.return_value = verify_ok
        return sw

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_old_active_computed_from_target_not_views(self, mock_bgm_cls, mock_vs_cls):
        """get_active_schema() n'est PAS appelé : old_active déduit depuis target_schema."""
        from amue.tasks.import_dag.switch_views import switch_views

        mgr = self._make_manager()
        mock_bgm_cls.return_value = mgr
        mock_vs_cls.return_value = self._make_switcher()

        switch_views.function({'target_schema': 'splus_green'})

        mgr.get_active_schema.assert_not_called()

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_rename_to_offline_uses_opposite_of_target_green(self, mock_bgm_cls, mock_vs_cls):
        """target=splus_green → rename_schema_to_offline('splus_blue')."""
        from amue.tasks.import_dag.switch_views import switch_views

        mgr = self._make_manager()
        mock_bgm_cls.return_value = mgr
        mock_vs_cls.return_value = self._make_switcher()

        switch_views.function({'target_schema': 'splus_green'})

        mgr.rename_schema_to_offline.assert_called_once_with('splus_blue')

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_rename_to_offline_uses_opposite_of_target_blue(self, mock_bgm_cls, mock_vs_cls):
        """target=splus_blue → rename_schema_to_offline('splus_green')."""
        from amue.tasks.import_dag.switch_views import switch_views

        mgr = self._make_manager()
        mock_bgm_cls.return_value = mgr
        mock_vs_cls.return_value = self._make_switcher()

        switch_views.function({'target_schema': 'splus_blue'})

        mgr.rename_schema_to_offline.assert_called_once_with('splus_green')

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_switch_views_success_calls_mark_completed_with_target(self, mock_bgm_cls, mock_vs_cls):
        """mark_import_completed(target_schema='splus_green') appelé après switch réussi."""
        from amue.tasks.import_dag.switch_views import switch_views

        mgr = self._make_manager()
        mock_bgm_cls.return_value = mgr
        mock_vs_cls.return_value = self._make_switcher()

        result = switch_views.function({'target_schema': 'splus_green'})

        mgr.mark_import_completed.assert_called_once_with(target_schema='splus_green')
        assert result['switched'] is True

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_switch_failure_raises_view_switch_error(self, mock_bgm_cls, mock_vs_cls):
        """switch_views_to_schema() retourne False → ViewSwitchError levée (tâche marquée failed)."""
        from amue.exceptions.bluegreen import ViewSwitchError
        from amue.tasks.import_dag.switch_views import switch_views

        mock_bgm_cls.return_value = self._make_manager()
        mock_vs_cls.return_value = self._make_switcher(switch_ok=False)

        with pytest.raises(ViewSwitchError):
            switch_views.function({'target_schema': 'splus_green'})

    @patch('amue.tasks.import_dag.switch_views.ViewSwitcher')
    @patch('amue.tasks.import_dag.switch_views.BlueGreenManager')
    def test_verification_failure_raises_view_switch_error(self, mock_bgm_cls, mock_vs_cls):
        """verify_views_point_to() retourne False → ViewSwitchError levée."""
        from amue.exceptions.bluegreen import ViewSwitchError
        from amue.tasks.import_dag.switch_views import switch_views

        mock_bgm_cls.return_value = self._make_manager()
        mock_vs_cls.return_value = self._make_switcher(switch_ok=True, verify_ok=False)

        with pytest.raises(ViewSwitchError):
            switch_views.function({'target_schema': 'splus_green'})
