"""
Tests unitaires pour BlueGreenState et BlueGreenStateManager.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestBlueGreenState:
    """Tests pour la dataclass BlueGreenState."""

    def test_to_dict_default(self):
        """to_dict() retourne tous les champs avec valeurs par défaut."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        state = BlueGreenState()
        result = state.to_dict()

        assert result["last_import_schema"] == ""
        assert result["last_switch_timestamp"] == ""
        assert result["last_sync_timestamp"] == ""
        assert result["import_in_progress"] is False
        assert result["import_started_at"] == ""
        assert result["import_correlation_id"] == ""

    def test_to_dict_with_values(self):
        """to_dict() retourne les valeurs correctement."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        state = BlueGreenState(
            last_import_schema="green",
            last_switch_timestamp="2026-03-16T02:00:00",
            last_sync_timestamp="2026-03-16T06:00:00",
            import_in_progress=True,
            import_started_at="2026-03-16T01:55:00",
            import_correlation_id="run-abc123",
        )
        result = state.to_dict()

        assert result["last_import_schema"] == "green"
        assert result["last_switch_timestamp"] == "2026-03-16T02:00:00"
        assert result["last_sync_timestamp"] == "2026-03-16T06:00:00"
        assert result["import_in_progress"] is True
        assert result["import_started_at"] == "2026-03-16T01:55:00"
        assert result["import_correlation_id"] == "run-abc123"

    def test_from_dict_full(self):
        """from_dict() recrée un état depuis un dictionnaire complet."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        data = {
            "last_import_schema": "blue",
            "last_switch_timestamp": "2026-03-16T02:00:00",
            "last_sync_timestamp": "2026-03-16T06:00:00",
            "import_in_progress": True,
            "import_started_at": "2026-03-16T01:55:00",
            "import_correlation_id": "run-xyz",
        }
        state = BlueGreenState.from_dict(data)

        assert state.last_import_schema == "blue"
        assert state.last_switch_timestamp == "2026-03-16T02:00:00"
        assert state.last_sync_timestamp == "2026-03-16T06:00:00"
        assert state.import_in_progress is True
        assert state.import_started_at == "2026-03-16T01:55:00"
        assert state.import_correlation_id == "run-xyz"

    def test_from_dict_defaults_on_missing_keys(self):
        """from_dict() utilise les valeurs par défaut pour les clés manquantes."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        state = BlueGreenState.from_dict({})

        assert state.last_import_schema == ""
        assert state.import_in_progress is False
        assert state.import_correlation_id == ""

    def test_from_dict_ignores_unknown_keys(self):
        """from_dict() ignore silencieusement les champs inconnus."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        data = {
            "last_import_schema": "green",
            "champ_inconnu": "valeur",
            "ancien_champ": True,
        }
        state = BlueGreenState.from_dict(data)

        assert state.last_import_schema == "green"
        assert not hasattr(state, "champ_inconnu")
        assert not hasattr(state, "ancien_champ")

    def test_to_dict_roundtrip(self):
        """to_dict() puis from_dict() reconstitue le même état."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        original = BlueGreenState(
            last_import_schema="green",
            import_in_progress=False,
            import_correlation_id="run-42",
        )
        restored = BlueGreenState.from_dict(original.to_dict())

        assert restored == original

    def test_frozen_immutable(self):
        """BlueGreenState est frozen — la mutation directe est impossible."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenState

        state = BlueGreenState()

        with pytest.raises(AttributeError):
            state.import_in_progress = True


class TestBlueGreenStateManagerLoadState:
    """Tests pour BlueGreenStateManager.load_state()."""

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_load_state_nominal(self, MockAdmin):
        """Retourne l'état chargé depuis la BDD."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        loaded = BlueGreenState(last_import_schema="green", import_in_progress=False)
        MockAdmin.return_value.get_bluegreen_state.return_value = loaded

        manager = BlueGreenStateManager()
        result = manager.load_state()

        assert result.last_import_schema == "green"
        assert result.import_in_progress is False

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_load_state_returns_default_when_none(self, MockAdmin):
        """Retourne l'état par défaut si AdminStateManager retourne None."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        MockAdmin.return_value.get_bluegreen_state.return_value = None

        manager = BlueGreenStateManager()
        result = manager.load_state()

        assert isinstance(result, BlueGreenState)
        assert result == BlueGreenState()

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_load_state_returns_default_on_db_error(self, MockAdmin):
        """Retourne l'état par défaut en cas d'erreur BDD (pas d'exception propagée)."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        MockAdmin.return_value.get_bluegreen_state.side_effect = Exception("DB error")

        manager = BlueGreenStateManager()
        result = manager.load_state()

        assert isinstance(result, BlueGreenState)
        assert result == BlueGreenState()


class TestBlueGreenStateManagerSaveState:
    """Tests pour BlueGreenStateManager.save_state()."""

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_save_state_nominal(self, MockAdmin):
        """Retourne True si la sauvegarde réussit."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        MockAdmin.return_value.save_bluegreen_state.return_value = True

        manager = BlueGreenStateManager()
        state = BlueGreenState(last_import_schema="blue")
        result = manager.save_state(state)

        assert result is True
        MockAdmin.return_value.save_bluegreen_state.assert_called_once_with(state)

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_save_state_returns_false_on_failure(self, MockAdmin):
        """Retourne False si AdminStateManager retourne False."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        MockAdmin.return_value.save_bluegreen_state.return_value = False

        manager = BlueGreenStateManager()
        result = manager.save_state(BlueGreenState())

        assert result is False

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_save_state_returns_false_on_db_error(self, MockAdmin):
        """Retourne False en cas d'exception BDD (pas de propagation)."""
        from common.application.bluegreen.bluegreen_state_manager import (
            BlueGreenState,
            BlueGreenStateManager,
        )

        MockAdmin.return_value.save_bluegreen_state.side_effect = Exception("DB error")

        manager = BlueGreenStateManager()
        result = manager.save_state(BlueGreenState())

        assert result is False


class TestBlueGreenStateManagerMarkers:
    """Tests pour les marqueurs mark_switch_completed et mark_sync_completed."""

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_mark_switch_completed_nominal(self, MockAdmin):
        """Délègue à AdminStateManager et retourne son résultat."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenStateManager

        MockAdmin.return_value.mark_switch_completed.return_value = True

        manager = BlueGreenStateManager()
        result = manager.mark_switch_completed("green")

        assert result is True
        MockAdmin.return_value.mark_switch_completed.assert_called_once_with("green")

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_mark_switch_completed_failure(self, MockAdmin):
        """Retourne False si AdminStateManager échoue."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenStateManager

        MockAdmin.return_value.mark_switch_completed.return_value = False

        manager = BlueGreenStateManager()
        result = manager.mark_switch_completed("blue")

        assert result is False

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_mark_sync_completed_nominal(self, MockAdmin):
        """Délègue à AdminStateManager et retourne True en cas de succès."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenStateManager

        MockAdmin.return_value.mark_sync_completed.return_value = True

        manager = BlueGreenStateManager()
        result = manager.mark_sync_completed()

        assert result is True
        MockAdmin.return_value.mark_sync_completed.assert_called_once()

    @patch('common.application.bluegreen.bluegreen_state_manager.AdminStateManager')
    def test_mark_sync_completed_failure(self, MockAdmin):
        """Retourne False si AdminStateManager échoue."""
        from common.application.bluegreen.bluegreen_state_manager import BlueGreenStateManager

        MockAdmin.return_value.mark_sync_completed.return_value = False

        manager = BlueGreenStateManager()
        result = manager.mark_sync_completed()

        assert result is False
