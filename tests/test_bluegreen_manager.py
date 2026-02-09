"""
Tests unitaires pour BlueGreenManager
"""
import pytest
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestBlueGreenState:
    """Tests pour la dataclass BlueGreenState"""

    def test_default_state(self):
        """État par défaut correctement initialisé"""
        from amue.services.bluegreen_manager import BlueGreenState

        state = BlueGreenState()

        assert state.active_schema == "blue"
        assert state.inactive_schema == "green"
        assert state.last_import_schema == ""
        assert state.import_in_progress is False
        assert state.rollback_available is False

    def test_to_dict(self):
        """Conversion en dictionnaire"""
        from amue.services.bluegreen_manager import BlueGreenState

        state = BlueGreenState(
            active_schema="green",
            inactive_schema="blue",
            rollback_available=True
        )

        result = state.to_dict()

        assert result["active_schema"] == "green"
        assert result["inactive_schema"] == "blue"
        assert result["rollback_available"] is True

    def test_from_dict(self):
        """Création depuis un dictionnaire"""
        from amue.services.bluegreen_manager import BlueGreenState

        data = {
            "active_schema": "green",
            "inactive_schema": "blue",
            "last_import_schema": "green",
            "import_in_progress": True,
            "rollback_available": True,
            "rollback_schema": "blue"
        }

        state = BlueGreenState.from_dict(data)

        assert state.active_schema == "green"
        assert state.inactive_schema == "blue"
        assert state.import_in_progress is True
        assert state.rollback_available is True


class TestBlueGreenManagerInit:
    """Tests pour l'initialisation de BlueGreenManager"""

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_is_enabled_true(self, mock_varmgr):
        """Mode activé si variable est 'true'"""
        mock_varmgr.get.return_value = "true"

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        assert manager.is_enabled() is True

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_is_enabled_false(self, mock_varmgr):
        """Mode désactivé si variable est 'false'"""
        mock_varmgr.get.return_value = "false"

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        assert manager.is_enabled() is False

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_is_enabled_default(self, mock_varmgr):
        """Mode désactivé par défaut"""
        mock_varmgr.get.return_value = "false"

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        assert manager.is_enabled() is False


class TestBlueGreenManagerState:
    """Tests pour la gestion de l'état"""

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_state_from_empty(self, mock_varmgr):
        """État par défaut si variable vide"""
        mock_varmgr.get.return_value = "{}"

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        state = manager.get_state()

        assert state.active_schema == "blue"
        assert state.inactive_schema == "green"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_state_from_json(self, mock_varmgr):
        """État chargé depuis JSON"""
        state_json = json.dumps({
            "active_schema": "green",
            "inactive_schema": "blue",
            "rollback_available": True
        })
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        state = manager.get_state()

        assert state.active_schema == "green"
        assert state.inactive_schema == "blue"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_target_schema_blue_active(self, mock_varmgr):
        """Schéma cible = green si blue actif"""
        state_json = json.dumps({"active_schema": "blue"})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        target = manager.get_target_schema()

        assert target == "splus_green"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_target_schema_green_active(self, mock_varmgr):
        """Schéma cible = blue si green actif"""
        state_json = json.dumps({"active_schema": "green"})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        target = manager.get_target_schema()

        assert target == "splus_blue"


class TestBlueGreenManagerMarkers:
    """Tests pour les marqueurs d'état"""

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_mark_import_started(self, mock_varmgr):
        """Marque le début de l'import"""
        mock_varmgr.get.return_value = "{}"
        mock_varmgr.set.return_value = True

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.mark_import_started()

        assert result is True
        assert manager.get_state().import_in_progress is True

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_mark_import_completed(self, mock_varmgr):
        """Marque la fin de l'import"""
        state_json = json.dumps({
            "active_schema": "blue",
            "import_in_progress": True
        })
        mock_varmgr.get.return_value = state_json
        mock_varmgr.set.return_value = True

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.mark_import_completed()

        assert result is True
        state = manager.get_state()
        assert state.import_in_progress is False
        assert state.last_import_schema == "green"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_mark_switch_completed(self, mock_varmgr):
        """Marque la fin du switch"""
        state_json = json.dumps({
            "active_schema": "blue",
            "inactive_schema": "green"
        })
        mock_varmgr.get.return_value = state_json
        mock_varmgr.set.return_value = True

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.mark_switch_completed()

        assert result is True
        state = manager.get_state()
        assert state.active_schema == "green"
        assert state.inactive_schema == "blue"
        assert state.rollback_available is True
        assert state.rollback_schema == "blue"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_mark_sync_completed(self, mock_varmgr):
        """Marque la fin de la sync"""
        state_json = json.dumps({"rollback_available": True})
        mock_varmgr.get.return_value = state_json
        mock_varmgr.set.return_value = True

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.mark_sync_completed()

        assert result is True
        state = manager.get_state()
        assert state.rollback_available is False
        assert state.last_sync_timestamp != ""


class TestBlueGreenManagerHelpers:
    """Tests pour les méthodes utilitaires"""

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_active_schema(self, mock_varmgr):
        """Retourne le schéma actif complet"""
        state_json = json.dumps({"active_schema": "blue"})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.get_active_schema()

        assert result == "splus_blue"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_inactive_schema(self, mock_varmgr):
        """Retourne le schéma inactif complet"""
        state_json = json.dumps({"inactive_schema": "green"})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.get_inactive_schema()

        assert result == "splus_green"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_view_schema(self, mock_varmgr):
        """Retourne le schéma des vues"""
        mock_varmgr.get.return_value = "{}"

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.get_view_schema()

        assert result == "splus"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_get_schema_for_table(self, mock_varmgr):
        """Retourne le nom qualifié de la table"""
        state_json = json.dumps({"active_schema": "blue"})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.get_schema_for_table("CSKS")

        assert result == "splus_green.csks"

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_needs_sync_true_if_rollback_available(self, mock_varmgr):
        """Sync nécessaire si rollback disponible"""
        state_json = json.dumps({"rollback_available": True})
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.needs_sync()

        assert result is True

    @patch('amue.services.bluegreen_manager.VarMgr')
    def test_needs_sync_false_after_sync(self, mock_varmgr):
        """Sync pas nécessaire si déjà sync"""
        state_json = json.dumps({
            "rollback_available": False,
            "last_sync_timestamp": "2024-01-15T10:00:00"
        })
        mock_varmgr.get.return_value = state_json

        from amue.services.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.needs_sync()

        assert result is False
