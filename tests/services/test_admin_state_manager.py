"""
Tests unitaires pour AdminStateManager
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone


def make_manager(mock_hook=None):
    from common.services.admin_state_manager import AdminStateManager
    if mock_hook is None:
        mock_hook = MagicMock()
    return AdminStateManager(postgres_hook=mock_hook), mock_hook


class TestAdminStateManagerTimestamps:
    """Tests pour les timestamps de synchro"""

    def test_get_last_finish_timestamp_string(self):
        """Retourne la valeur ISO 8601 sous forme de chaîne"""
        manager, hook = make_manager()
        hook.get_first.return_value = ('2026-02-17T10:18:22+00:00',)

        result = manager.get_last_finish_timestamp()

        assert result == '2026-02-17T10:18:22+00:00'

    def test_get_last_finish_timestamp_datetime(self):
        """Convertit un objet datetime en ISO 8601"""
        manager, hook = make_manager()
        ts = datetime(2026, 2, 17, 10, 18, 22, tzinfo=timezone.utc)
        hook.get_first.return_value = (ts,)

        result = manager.get_last_finish_timestamp()

        assert '2026-02-17' in result

    def test_get_last_finish_timestamp_none(self):
        """Retourne None si jamais enregistré"""
        manager, hook = make_manager()
        hook.get_first.return_value = (None,)

        result = manager.get_last_finish_timestamp()

        assert result is None

    def test_get_last_finish_timestamp_error(self):
        """Retourne None en cas d'erreur BDD"""
        manager, hook = make_manager()
        hook.get_first.side_effect = Exception("DB error")

        result = manager.get_last_finish_timestamp()

        assert result is None

    def test_set_last_finish_timestamp(self):
        """Enregistre le timestamp finish"""
        manager, hook = make_manager()

        manager.set_last_finish_timestamp('2026-02-17T10:18:22+00:00')

        hook.run.assert_called_once()
        call_args = hook.run.call_args
        assert call_args[1]['parameters'][0] == '2026-02-17T10:18:22+00:00'

    def test_get_last_successful_run_string(self):
        """Retourne la valeur ISO 8601"""
        manager, hook = make_manager()
        hook.get_first.return_value = ('2026-02-17T12:00:00',)

        result = manager.get_last_successful_run()

        assert result == '2026-02-17T12:00:00'

    def test_get_last_successful_run_none(self):
        """Retourne None si jamais enregistré"""
        manager, hook = make_manager()
        hook.get_first.return_value = (None,)

        result = manager.get_last_successful_run()

        assert result is None

    def test_set_last_successful_run(self):
        """Enregistre la date de dernier succès"""
        manager, hook = make_manager()

        manager.set_last_successful_run('2026-02-17T12:00:00')

        hook.run.assert_called_once()
        call_args = hook.run.call_args
        assert call_args[1]['parameters'][0] == '2026-02-17T12:00:00'


class TestAdminStateManagerReportStart:
    """Tests pour get/set last_report_start"""

    def test_get_last_report_start_string(self):
        """Retourne la valeur ISO 8601"""
        manager, hook = make_manager()
        hook.get_first.return_value = ('2026-02-17T10:08:19+00:00',)

        result = manager.get_last_report_start()

        assert result == '2026-02-17T10:08:19+00:00'

    def test_get_last_report_start_datetime(self):
        """Convertit un objet datetime en ISO 8601"""
        manager, hook = make_manager()
        ts = datetime(2026, 2, 17, 10, 8, 19, tzinfo=timezone.utc)
        hook.get_first.return_value = (ts,)

        result = manager.get_last_report_start()

        assert '2026-02-17' in result

    def test_get_last_report_start_none(self):
        """Retourne None si jamais enregistré"""
        manager, hook = make_manager()
        hook.get_first.return_value = (None,)

        result = manager.get_last_report_start()

        assert result is None

    def test_get_last_report_start_no_row(self):
        """Retourne None si aucune ligne trouvée"""
        manager, hook = make_manager()
        hook.get_first.return_value = None

        result = manager.get_last_report_start()

        assert result is None

    def test_get_last_report_start_error(self):
        """Retourne None en cas d'erreur BDD"""
        manager, hook = make_manager()
        hook.get_first.side_effect = Exception("DB error")

        result = manager.get_last_report_start()

        assert result is None

    def test_set_last_report_start(self):
        """Enregistre le timestamp de début du rapport"""
        manager, hook = make_manager()

        manager.set_last_report_start('2026-02-17T10:08:19+00:00')

        hook.run.assert_called_once()
        call_args = hook.run.call_args
        assert call_args[1]['parameters'][0] == '2026-02-17T10:08:19+00:00'

    def test_set_last_report_start_error_silent(self):
        """Erreur BDD ne propage pas d'exception"""
        manager, hook = make_manager()
        hook.run.side_effect = Exception("DB error")

        # Ne doit pas lever d'exception
        manager.set_last_report_start('2026-02-17T10:08:19+00:00')


class TestAdminStateManagerImportLock:
    """Tests pour la gestion du verrou d'import"""

    def test_try_acquire_import_lock_success(self):
        """Acquiert le verrou si non verrouillé"""
        manager, hook = make_manager()
        hook.get_records.return_value = [(1,)]

        result = manager.try_acquire_import_lock('2026-02-17T10:00:00', 'corr-123')

        assert result is True

    def test_try_acquire_import_lock_already_locked(self):
        """Ne peut pas acquérir le verrou si déjà verrouillé"""
        manager, hook = make_manager()
        hook.get_records.return_value = []

        result = manager.try_acquire_import_lock('2026-02-17T10:00:00', 'corr-123')

        assert result is False

    def test_try_acquire_import_lock_error(self):
        """Retourne False en cas d'erreur BDD"""
        manager, hook = make_manager()
        hook.get_records.side_effect = Exception("DB error")

        result = manager.try_acquire_import_lock('2026-02-17T10:00:00', 'corr-123')

        assert result is False

    def test_release_import_lock_success(self):
        """Libère le verrou quand il est tenu (RETURNING retourne une ligne)"""
        manager, hook = make_manager()
        hook.get_first.return_value = (1,)  # UPDATE ... RETURNING id → une ligne

        result = manager.release_import_lock('blue')

        assert result is True
        hook.get_first.assert_called_once()

    def test_release_import_lock_error(self):
        """Reraise l'exception en cas d'erreur BDD (verrou potentiellement encore actif)"""
        import pytest
        manager, hook = make_manager()
        hook.get_first.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            manager.release_import_lock('blue')

    def test_release_lock_not_held_returns_false(self):
        """Retourne False si le verrou n'était pas tenu (RETURNING retourne None)"""
        manager, hook = make_manager()
        hook.get_first.return_value = None  # Aucune ligne mise à jour

        result = manager.release_import_lock('blue')

        assert result is False

    def test_release_lock_not_held_logs_warning(self):
        """Log un warning si le verrou n'était pas tenu"""
        from unittest.mock import patch
        manager, hook = make_manager()
        hook.get_first.return_value = None

        with patch('common.services.admin_state_manager.logger') as mock_logger:
            manager.release_import_lock('blue')

        mock_logger.warning.assert_called_once()
        assert 'non tenu' in mock_logger.warning.call_args[0][0]

    def test_force_release_lock(self):
        """Force la libération du verrou"""
        manager, hook = make_manager()

        result = manager.force_release_lock()

        assert result is True
        hook.run.assert_called_once()
