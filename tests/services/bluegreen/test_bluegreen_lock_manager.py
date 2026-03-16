"""
Tests unitaires pour BlueGreenLockManager.
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


def _make_state(import_in_progress=False, import_started_at="", import_correlation_id=""):
    from amue.services.bluegreen.bluegreen_state_manager import BlueGreenState
    return BlueGreenState(
        import_in_progress=import_in_progress,
        import_started_at=import_started_at,
        import_correlation_id=import_correlation_id,
    )


class TestBlueGreenLockManagerAcquire:
    """Tests pour acquire_lock()."""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_acquire_lock_success(self, MockAdmin):
        """Acquiert le verrou quand la BDD l'accorde au premier essai."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        MockAdmin.return_value.try_acquire_import_lock.return_value = True
        state = _make_state(import_in_progress=False)

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.acquire_lock("run-abc123", state)

        assert result is True
        MockAdmin.return_value.try_acquire_import_lock.assert_called_once()

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_acquire_lock_stale_auto_released(self, MockAdmin):
        """
        Acquiert le verrou après libération automatique d'un verrou abandonné (stale).
        """
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        # Premier try_acquire échoue (verrou tenu), second réussit après force_release
        MockAdmin.return_value.try_acquire_import_lock.side_effect = [False, True]
        MockAdmin.return_value.force_release_lock.return_value = True

        # Verrou démarré il y a 3 heures → stale (timeout = 120 min)
        stale_started = (datetime.now() - timedelta(hours=3)).isoformat()
        state = _make_state(import_in_progress=True, import_started_at=stale_started)

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.acquire_lock("run-new", state)

        assert result is True
        MockAdmin.return_value.force_release_lock.assert_called_once()
        assert MockAdmin.return_value.try_acquire_import_lock.call_count == 2

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_acquire_lock_raises_concurrent_import_error(self, MockAdmin):
        """Lève ConcurrentImportError si le verrou est actif et non-stale."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager
        from amue.exceptions import ConcurrentImportError

        MockAdmin.return_value.try_acquire_import_lock.return_value = False

        # Verrou récent (5 minutes) → pas stale
        recent_started = (datetime.now() - timedelta(minutes=5)).isoformat()
        state = _make_state(
            import_in_progress=True,
            import_started_at=recent_started,
            import_correlation_id="run-other",
        )

        lock_mgr = BlueGreenLockManager()

        with pytest.raises(ConcurrentImportError):
            lock_mgr.acquire_lock("run-new", state)

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_acquire_lock_stale_second_try_fails_raises_error(self, MockAdmin):
        """Lève ConcurrentImportError si le second try_acquire échoue après force_release."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager
        from amue.exceptions import ConcurrentImportError

        MockAdmin.return_value.try_acquire_import_lock.return_value = False
        MockAdmin.return_value.force_release_lock.return_value = True

        stale_started = (datetime.now() - timedelta(hours=3)).isoformat()
        state = _make_state(import_in_progress=True, import_started_at=stale_started)

        lock_mgr = BlueGreenLockManager()

        with pytest.raises(ConcurrentImportError):
            lock_mgr.acquire_lock("run-new", state)


class TestBlueGreenLockManagerRelease:
    """Tests pour release_lock()."""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_release_lock_success(self, MockAdmin):
        """Libère le verrou et retourne True."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        MockAdmin.return_value.release_import_lock.return_value = True
        state = _make_state(
            import_in_progress=True,
            import_started_at="2026-03-16T01:55:00",
            import_correlation_id="run-abc123",
        )

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.release_lock("green", state)

        assert result is True
        MockAdmin.return_value.release_import_lock.assert_called_once_with("green")

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_release_lock_not_held_returns_false(self, MockAdmin):
        """Retourne False si le verrou n'était pas tenu."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        MockAdmin.return_value.release_import_lock.return_value = False
        state = _make_state()

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.release_lock("blue", state)

        assert result is False


class TestBlueGreenLockManagerForceRelease:
    """Tests pour force_release()."""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_force_release_success(self, MockAdmin):
        """Force la libération et retourne True."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        MockAdmin.return_value.force_release_lock.return_value = True

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.force_release()

        assert result is True
        MockAdmin.return_value.force_release_lock.assert_called_once()

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_force_release_failure(self, MockAdmin):
        """Retourne False si AdminStateManager échoue."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        MockAdmin.return_value.force_release_lock.return_value = False

        lock_mgr = BlueGreenLockManager()
        result = lock_mgr.force_release()

        assert result is False


class TestBlueGreenLockManagerIsStale:
    """Tests pour is_stale()."""

    def test_is_stale_true_when_expired(self):
        """Retourne True si le verrou date de plus de BLUEGREEN_LOCK_TIMEOUT_MINUTES."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        stale_started = (datetime.now() - timedelta(hours=3)).isoformat()
        state = _make_state(import_in_progress=True, import_started_at=stale_started)

        lock_mgr = BlueGreenLockManager()
        assert lock_mgr.is_stale(state) is True

    def test_is_stale_false_when_recent(self):
        """Retourne False si le verrou est récent (< timeout)."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        recent_started = (datetime.now() - timedelta(minutes=5)).isoformat()
        state = _make_state(import_in_progress=True, import_started_at=recent_started)

        lock_mgr = BlueGreenLockManager()
        assert lock_mgr.is_stale(state) is False

    def test_is_stale_false_when_no_lock(self):
        """Retourne False si import_in_progress est False."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        state = _make_state(import_in_progress=False, import_started_at="2026-03-16T01:00:00")

        lock_mgr = BlueGreenLockManager()
        assert lock_mgr.is_stale(state) is False

    def test_is_stale_false_when_no_started_at(self):
        """Retourne False si import_started_at est vide."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        state = _make_state(import_in_progress=True, import_started_at="")

        lock_mgr = BlueGreenLockManager()
        assert lock_mgr.is_stale(state) is False

    def test_is_stale_true_when_date_invalid(self):
        """Retourne True si import_started_at n'est pas une date ISO valide."""
        from amue.services.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

        state = _make_state(import_in_progress=True, import_started_at="not-a-date")

        lock_mgr = BlueGreenLockManager()
        assert lock_mgr.is_stale(state) is True
