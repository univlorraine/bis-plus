"""
Tests unitaires pour BlueGreenManager
"""
import pytest
from dataclasses import replace
from unittest.mock import MagicMock, patch


class TestBlueGreenState:
    """Tests pour la dataclass BlueGreenState"""

    def test_default_state(self):
        """État par défaut correctement initialisé"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        state = BlueGreenState()

        assert state.last_import_schema == ""
        assert state.import_in_progress is False

    def test_to_dict(self):
        """Conversion en dictionnaire"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        state = BlueGreenState(last_import_schema="green")

        result = state.to_dict()

        assert result["last_import_schema"] == "green"
        assert "rollback_available" not in result
        assert "active_schema" not in result
        assert "inactive_schema" not in result
        assert "rollback_schema" not in result

    def test_from_dict(self):
        """Création depuis un dictionnaire"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        data = {
            "last_import_schema": "green",
            "import_in_progress": True,
        }

        state = BlueGreenState.from_dict(data)

        assert state.last_import_schema == "green"
        assert state.import_in_progress is True

    def test_from_dict_ignores_old_schema_fields(self):
        """from_dict ignore silencieusement les anciens champs schema"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        # Ancien format avec les champs supprimés
        data = {
            "active_schema": "blue",
            "inactive_schema": "green",
            "rollback_schema": "green",
            "rollback_available": True,
        }

        state = BlueGreenState.from_dict(data)

        # Les anciens champs ne doivent pas exister sur la dataclass
        assert not hasattr(state, 'active_schema')
        assert not hasattr(state, 'rollback_available')

    def test_frozen_state_is_immutable(self):
        """BlueGreenState est frozen - mutation directe impossible."""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        state = BlueGreenState()

        with pytest.raises(AttributeError):
            state.import_in_progress = True

    def test_frozen_state_replace_creates_new_instance(self):
        """replace() crée une nouvelle instance sans modifier l'originale."""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState

        state = BlueGreenState(import_in_progress=False)
        new_state = replace(state, import_in_progress=True)

        assert state.import_in_progress is False  # Original inchangé
        assert new_state.import_in_progress is True  # Nouvelle instance
        assert state is not new_state


class TestBlueGreenManagerState:
    """Tests pour la gestion de l'état"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_get_state_from_empty(self, mock_admin_cls):
        """État par défaut si AdminStateManager retourne un état vide"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState()

        manager = BlueGreenManager()
        state = manager.get_state()

        assert state.import_in_progress is False

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_get_state_from_json(self, mock_admin_cls):
        """État chargé depuis la BDD"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(last_import_schema="green")

        manager = BlueGreenManager()
        state = manager.get_state()

        assert state.last_import_schema == "green"

    def test_get_target_schema_blue_active(self):
        """Schéma cible = green si views pointent vers blue"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"
        manager._view_switcher = mock_vs

        target = manager.get_target_schema()

        assert target == "splus_green"

    def test_get_target_schema_green_active(self):
        """Schéma cible = blue si views pointent vers green"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_green"
        manager._view_switcher = mock_vs

        target = manager.get_target_schema()

        assert target == "splus_blue"

    def test_get_target_schema_no_views(self):
        """Schéma cible = blue si aucune vue (premier import)"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = None
        manager._view_switcher = mock_vs

        target = manager.get_target_schema()

        assert target == "splus_blue"


class TestBlueGreenManagerMarkers:
    """Tests pour les marqueurs d'état"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_import_started(self, mock_admin_cls):
        """Marque le début de l'import"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.try_acquire_import_lock.return_value = True
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(import_in_progress=True)

        manager = BlueGreenManager()
        result = manager.mark_import_started()

        assert result is True
        assert manager.get_state().import_in_progress is True

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_import_completed(self, mock_admin_cls):
        """Marque la fin de l'import"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        # Premier appel : état initial avec import en cours
        # Deuxième appel (assertion) : import terminé avec schéma mis à jour
        mock_admin.get_bluegreen_state.side_effect = [
            BlueGreenState(import_in_progress=True),
            BlueGreenState(import_in_progress=False, last_import_schema="green"),
        ]
        mock_admin.release_import_lock.return_value = True

        manager = BlueGreenManager()
        # Views pointent vers blue → target = green → last_import = "green"
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"
        manager._view_switcher = mock_vs

        result = manager.mark_import_completed()

        assert result is True
        state = manager.get_state()
        assert state.import_in_progress is False
        assert state.last_import_schema == "green"

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_import_completed_with_explicit_target(self, mock_admin_cls):
        """target_schema explicite → correct active_schema passé à release_lock, sans lire les vues"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(import_in_progress=True)
        mock_admin.release_import_lock.return_value = True

        manager = BlueGreenManager()
        result = manager.mark_import_completed(target_schema='splus_green')

        assert result is True
        mock_admin.release_import_lock.assert_called_once_with('green')
        # ViewSwitcher n'a jamais été instancié (pas de lecture de vues)
        assert manager._view_switcher is None

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_import_completed_without_target_reads_views(self, mock_admin_cls):
        """Sans target_schema → fallback sur get_target_schema() (lecture des vues)"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(import_in_progress=True)
        mock_admin.release_import_lock.return_value = True

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = 'splus_blue'  # views → blue → target = green
        manager._view_switcher = mock_vs

        result = manager.mark_import_completed()

        assert result is True
        mock_vs.get_current_target_schema.assert_called()
        mock_admin.release_import_lock.assert_called_once_with('green')

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_switch_completed(self, mock_admin_cls):
        """Marque la fin du switch"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.mark_switch_completed.return_value = True
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(
            last_switch_timestamp="2024-01-15T10:00:00"
        )

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"
        manager._view_switcher = mock_vs

        result = manager.mark_switch_completed()

        assert result is True
        state = manager.get_state()
        assert state.last_switch_timestamp != ""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_mark_sync_completed(self, mock_admin_cls):
        """Marque la fin de la sync"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.mark_sync_completed.return_value = True
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(
            last_sync_timestamp="2024-01-15T10:00:00"
        )

        manager = BlueGreenManager()
        result = manager.mark_sync_completed()

        assert result is True
        state = manager.get_state()
        assert state.last_sync_timestamp != ""

class TestBlueGreenManagerHelpers:
    """Tests pour les méthodes utilitaires"""

    def test_get_active_schema_blue(self):
        """Retourne le schéma actif complet quand views pointent vers blue"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"
        manager._view_switcher = mock_vs

        result = manager.get_active_schema()

        assert result == "splus_blue"

    def test_get_active_schema_no_views(self):
        """Retourne splus_green si aucune vue (premier import)"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = None
        manager._view_switcher = mock_vs

        result = manager.get_active_schema()

        assert result == "splus_green"

    def test_get_inactive_schema(self):
        """Retourne le schéma inactif complet (opposé de l'actif)"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"  # active=blue → inactive=green
        manager._view_switcher = mock_vs

        result = manager.get_inactive_schema()

        assert result == "splus_green"

    def test_get_view_schema(self):
        """Retourne le schéma des vues"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        result = manager.get_view_schema()

        assert result == "splus"

    def test_get_schema_for_table(self):
        """Retourne le nom qualifié de la table dans le schéma cible"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager

        manager = BlueGreenManager()
        mock_vs = MagicMock()
        mock_vs.get_current_target_schema.return_value = "splus_blue"  # active=blue → target=green
        manager._view_switcher = mock_vs

        result = manager.get_schema_for_table("CSKS")

        assert result == "splus_green.csks"

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_needs_sync_true_if_no_sync(self, mock_admin_cls):
        """Sync nécessaire si pas de last_sync_timestamp"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState()

        manager = BlueGreenManager()
        result = manager.needs_sync()

        assert result is True

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_needs_sync_false_after_sync(self, mock_admin_cls):
        """Sync pas nécessaire si déjà sync"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_bluegreen_state.return_value = BlueGreenState(
            last_sync_timestamp="2024-01-15T10:00:00"
        )

        manager = BlueGreenManager()
        result = manager.needs_sync()

        assert result is False


class TestBlueGreenManagerOfflineRename:
    """Tests pour le renommage offline du schéma inactif"""

    def _make_manager(self, mock_hook):
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
        manager = BlueGreenManager(postgres_hook=mock_hook)
        return manager

    def test_schema_exists_true(self):
        """schema_exists retourne True si le schéma est trouvé"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(1,)]

        manager = self._make_manager(mock_hook)
        result = manager.schema_exists("splus_blue")

        assert result is True
        mock_hook.get_records.assert_called_once()

    def test_schema_exists_false(self):
        """schema_exists retourne False si le schéma est absent"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []

        manager = self._make_manager(mock_hook)
        result = manager.schema_exists("splus_blue")

        assert result is False

    def test_rename_schema_to_offline_success(self):
        """rename_schema_to_offline renomme le schéma si il existe"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(1,)]  # schema exists

        manager = self._make_manager(mock_hook)
        result = manager.rename_schema_to_offline("splus_blue")

        assert result is True
        mock_hook.run.assert_called_once()

    def test_rename_schema_to_offline_schema_missing(self):
        """rename_schema_to_offline retourne False si le schéma n'existe pas"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []  # schema absent

        manager = self._make_manager(mock_hook)
        result = manager.rename_schema_to_offline("splus_blue")

        assert result is False
        mock_hook.run.assert_not_called()

    def test_rename_schema_from_offline_success(self):
        """rename_schema_from_offline restaure le schéma si le variant offline existe"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [(1,)]  # offline schema exists

        manager = self._make_manager(mock_hook)
        result = manager.rename_schema_from_offline("splus_blue")

        assert result is True
        mock_hook.run.assert_called_once()

    def test_rename_schema_from_offline_no_offline(self):
        """rename_schema_from_offline est no-op si le variant offline n'existe pas"""
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []  # pas d'offline

        manager = self._make_manager(mock_hook)
        result = manager.rename_schema_from_offline("splus_blue")

        assert result is False
        mock_hook.run.assert_not_called()

    def test_offline_suffix_constant(self):
        """La constante OFFLINE_SUFFIX est bien définie"""
        from amue.services.bluegreen.bluegreen_manager import BlueGreenManager
        assert BlueGreenManager.OFFLINE_SUFFIX == "_offline"
