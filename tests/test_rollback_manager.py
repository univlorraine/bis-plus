"""
Tests unitaires pour RollbackManager
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestRollbackManagerInit:
    """Tests pour l'initialisation de RollbackManager"""

    @patch('amue.services.rollback_manager.ViewSwitcher')
    @patch('amue.services.rollback_manager.BlueGreenManager')
    def test_init_default_managers(self, mock_bg_class, mock_vs_class):
        """Crée les managers par défaut"""
        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager()

        mock_bg_class.assert_called_once()
        mock_vs_class.assert_called_once()

    def test_init_custom_managers(self):
        """Utilise les managers personnalisés"""
        mock_bg = MagicMock()
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)

        assert manager.bluegreen_manager == mock_bg
        assert manager.view_switcher == mock_vs


class TestRollbackManagerCanRollback:
    """Tests pour can_rollback"""

    def test_can_rollback_disabled(self):
        """Retourne False si blue/green désactivé"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = False
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.can_rollback()

        assert result is False

    def test_can_rollback_import_in_progress(self):
        """Retourne False si import en cours"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = True
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.can_rollback()

        assert result is False

    def test_can_rollback_not_available(self):
        """Retourne False si rollback non disponible"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.is_rollback_available.return_value = False
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.can_rollback()

        assert result is False

    def test_can_rollback_true(self):
        """Retourne True si toutes les conditions sont remplies"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.is_rollback_available.return_value = True
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.can_rollback()

        assert result is True


class TestRollbackManagerGetInfo:
    """Tests pour get_rollback_info"""

    def test_get_rollback_info_available(self):
        """Retourne les infos quand rollback disponible"""
        mock_state = MagicMock()
        mock_state.active_schema = "green"
        mock_state.rollback_schema = "blue"
        mock_state.last_switch_timestamp = "2024-01-15T10:00:00"
        mock_state.rollback_available = True

        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.get_state.return_value = mock_state
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        info = manager.get_rollback_info()

        assert info['available'] is True
        assert info['rollback_schema'] == 'splus_blue'
        assert info['current_schema'] == 'splus_green'

    def test_get_rollback_info_disabled(self):
        """Retourne raison si désactivé"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = False
        mock_bg.get_state.return_value = MagicMock()
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        info = manager.get_rollback_info()

        assert info['available'] is False
        assert 'désactivé' in info['reason'].lower()


class TestRollbackManagerRollback:
    """Tests pour rollback"""

    def test_rollback_success(self):
        """Rollback réussi"""
        mock_state = MagicMock()
        mock_state.active_schema = "green"
        mock_state.rollback_schema = "blue"
        mock_state.rollback_available = True

        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.is_rollback_available.return_value = True
        mock_bg.get_state.return_value = mock_state

        mock_vs = MagicMock()
        mock_vs.switch_views_to_schema.return_value = True

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.rollback()

        assert result['success'] is True
        assert result['previous_schema'] == 'splus_green'
        assert result['new_schema'] == 'splus_blue'
        mock_bg.mark_rollback_completed.assert_called_once()

    def test_rollback_not_available(self):
        """Rollback échoue si non disponible"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = False
        mock_bg.get_state.return_value = MagicMock(rollback_available=False)
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.rollback()

        assert result['success'] is False
        assert result['error'] is not None

    def test_rollback_switch_fails(self):
        """Rollback échoue si switch échoue"""
        mock_state = MagicMock()
        mock_state.active_schema = "green"
        mock_state.rollback_schema = "blue"
        mock_state.rollback_available = True

        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.is_rollback_available.return_value = True
        mock_bg.get_state.return_value = mock_state

        mock_vs = MagicMock()
        mock_vs.switch_views_to_schema.return_value = False

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.rollback()

        assert result['success'] is False
        assert 'switch' in result['error'].lower()


class TestRollbackManagerPreview:
    """Tests pour preview_rollback"""

    def test_preview_rollback_available(self):
        """Prévisualisation quand rollback disponible"""
        mock_state = MagicMock()
        mock_state.active_schema = "green"
        mock_state.rollback_schema = "blue"
        mock_state.rollback_available = True
        mock_state.last_switch_timestamp = "2024-01-15T10:00:00"

        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.is_import_in_progress.return_value = False
        mock_bg.get_state.return_value = mock_state
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        preview = manager.preview_rollback()

        assert preview['would_rollback'] is True
        assert preview['from_schema'] == 'splus_green'
        assert preview['to_schema'] == 'splus_blue'

    def test_preview_rollback_not_available(self):
        """Prévisualisation quand rollback non disponible"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = False
        mock_bg.get_state.return_value = MagicMock(rollback_available=False)
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        preview = manager.preview_rollback()

        assert preview['would_rollback'] is False
        assert preview['reason_if_not_available'] is not None


class TestRollbackManagerForce:
    """Tests pour force_rollback"""

    def test_force_rollback_success(self):
        """Force rollback réussi"""
        mock_state = MagicMock()
        mock_state.active_schema = "green"

        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = True
        mock_bg.get_state.return_value = mock_state

        mock_vs = MagicMock()
        mock_vs.switch_views_to_schema.return_value = True

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.force_rollback()

        assert result['success'] is True
        assert result['forced'] is True

    def test_force_rollback_disabled(self):
        """Force rollback échoue si blue/green désactivé"""
        mock_bg = MagicMock()
        mock_bg.is_enabled.return_value = False
        mock_vs = MagicMock()

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.force_rollback()

        assert result['success'] is False


class TestRollbackManagerVerify:
    """Tests pour verify_rollback_integrity"""

    def test_verify_rollback_integrity_ok(self):
        """Vérification OK"""
        mock_state = MagicMock()
        mock_state.active_schema = "blue"

        mock_bg = MagicMock()
        mock_bg.get_state.return_value = mock_state

        mock_vs = MagicMock()
        mock_vs.verify_views_point_to.return_value = True
        mock_vs.get_current_target_schema.return_value = 'splus_blue'

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.verify_rollback_integrity()

        assert result['verified'] is True
        assert result['expected_schema'] == 'splus_blue'

    def test_verify_rollback_integrity_failed(self):
        """Vérification échouée"""
        mock_state = MagicMock()
        mock_state.active_schema = "blue"

        mock_bg = MagicMock()
        mock_bg.get_state.return_value = mock_state

        mock_vs = MagicMock()
        mock_vs.verify_views_point_to.return_value = False
        mock_vs.get_current_target_schema.return_value = 'splus_green'

        from amue.services.rollback_manager import RollbackManager

        manager = RollbackManager(bluegreen_manager=mock_bg, view_switcher=mock_vs)
        result = manager.verify_rollback_integrity()

        assert result['verified'] is False
