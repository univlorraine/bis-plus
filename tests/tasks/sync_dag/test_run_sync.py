# tests/tasks/sync_dag/test_run_sync.py
"""Tests unitaires pour la task run_sync."""
from unittest.mock import MagicMock, patch


class TestRunSyncDisabled:
    """Tests quand blue/green est désactivé."""

    def test_returns_skipped_when_disabled(self):
        """Retourne status=skipped si enabled=False."""
        from amue.tasks.sync_dag.run_sync import run_sync

        result = run_sync.function({'enabled': False})

        assert result['status'] == 'skipped'
        assert result['reason'] == 'bluegreen_disabled'
        assert result['tables_synced'] == 0
        assert result['tables_failed'] == 0
        assert result['total_rows_copied'] == 0

    def test_skipped_does_not_call_synchronizer(self):
        """Pas d'appel au SchemaSynchronizer si désactivé."""
        with patch('amue.tasks.sync_dag.run_sync.SchemaSynchronizer') as MockSync:
            from amue.tasks.sync_dag.run_sync import run_sync
            run_sync.function({'enabled': False})
            MockSync.assert_not_called()


class TestRunSyncSuccess:
    """Tests pour une synchronisation réussie."""

    def _run_with_result(self, sync_status='success', source='splus_blue', target='splus_green'):
        sync_result = {
            'status': sync_status,
            'tables_synced': 3,
            'tables_failed': 0,
            'total_rows_copied': 15000,
            'details': [],
        }
        with patch('amue.tasks.sync_dag.run_sync.SchemaSynchronizer') as MockSync, \
             patch('amue.tasks.sync_dag.run_sync.BlueGreenManager') as MockBGM:
            mock_sync_instance = MagicMock()
            MockSync.return_value = mock_sync_instance
            mock_sync_instance.sync_schemas.return_value = sync_result

            from amue.tasks.sync_dag.run_sync import run_sync
            result = run_sync.function({
                'enabled': True,
                'source_schema': source,
                'target_schema': target,
            })
            return result, MockSync, MockBGM

    def test_calls_sync_schemas_with_correct_args(self):
        """sync_schemas est appelé avec source et target."""
        _, MockSync, _ = self._run_with_result()
        MockSync.return_value.sync_schemas.assert_called_once_with('splus_blue', 'splus_green')

    def test_returns_synchronizer_result(self):
        """Retourne le résultat du SchemaSynchronizer."""
        result, _, _ = self._run_with_result()
        assert result['status'] == 'success'
        assert result['tables_synced'] == 3
        assert result['total_rows_copied'] == 15000

    def test_marks_sync_completed_on_success(self):
        """mark_sync_completed() est appelé après un succès."""
        _, _, MockBGM = self._run_with_result(sync_status='success')
        MockBGM.return_value.mark_sync_completed.assert_called_once()

    def test_renames_schema_to_offline_on_success(self):
        """rename_schema_to_offline() est appelé sur le schéma cible."""
        _, _, MockBGM = self._run_with_result(sync_status='success')
        MockBGM.return_value.rename_schema_to_offline.assert_called_once_with('splus_green')

    def test_partial_status_also_triggers_offline(self):
        """rename_schema_to_offline() est aussi appelé sur status=partial."""
        _, _, MockBGM = self._run_with_result(sync_status='partial')
        MockBGM.return_value.rename_schema_to_offline.assert_called_once_with('splus_green')


class TestRunSyncFailure:
    """Tests pour une synchronisation échouée."""

    def test_does_not_rename_schema_on_failure(self):
        """rename_schema_to_offline() n'est PAS appelé en cas d'échec."""
        sync_result = {
            'status': 'error',
            'tables_synced': 0,
            'tables_failed': 3,
            'total_rows_copied': 0,
            'details': [],
        }
        with patch('amue.tasks.sync_dag.run_sync.SchemaSynchronizer') as MockSync, \
             patch('amue.tasks.sync_dag.run_sync.BlueGreenManager') as MockBGM:
            MockSync.return_value.sync_schemas.return_value = sync_result

            from amue.tasks.sync_dag.run_sync import run_sync
            result = run_sync.function({
                'enabled': True,
                'source_schema': 'splus_blue',
                'target_schema': 'splus_green',
            })

            MockBGM.return_value.rename_schema_to_offline.assert_not_called()
            assert result['status'] == 'error'

    def test_does_not_mark_completed_on_failure(self):
        """mark_sync_completed() n'est pas appelé si la sync a échoué."""
        sync_result = {'status': 'error', 'tables_synced': 0, 'tables_failed': 1,
                       'total_rows_copied': 0, 'details': []}
        with patch('amue.tasks.sync_dag.run_sync.SchemaSynchronizer') as MockSync, \
             patch('amue.tasks.sync_dag.run_sync.BlueGreenManager') as MockBGM:
            MockSync.return_value.sync_schemas.return_value = sync_result

            from amue.tasks.sync_dag.run_sync import run_sync
            run_sync.function({
                'enabled': True,
                'source_schema': 'splus_blue',
                'target_schema': 'splus_green',
            })

            MockBGM.return_value.mark_sync_completed.assert_not_called()
