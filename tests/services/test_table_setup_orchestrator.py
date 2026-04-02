"""Tests unitaires pour TableSetupOrchestrator."""
from unittest.mock import MagicMock, patch


def _run_orchestrator(table_info, verifier_result, manage_result=None):
    """
    Helper : crée un orchestrateur avec mocks actifs, appelle run() et retourne
    (result, config_manager).
    """
    from amue.services.table_setup_orchestrator import TableSetupOrchestrator

    api_hook = MagicMock()
    config_manager = MagicMock()

    with patch('amue.services.table_setup_orchestrator.AMUETableVerifier') as MockVerifier, \
         patch('amue.services.table_setup_orchestrator.AMUETableManager') as MockManager:

        MockVerifier.return_value.verify_structure.return_value = verifier_result
        MockManager.return_value.manage_table.return_value = manage_result or {'created': True}

        orch = TableSetupOrchestrator(
            api_hook=api_hook,
            table_config_manager=config_manager,
        )
        result = orch.run(table_info)

    return result, config_manager


class TestTableSetupOrchestratorSuccess:

    def test_new_table_success(self):
        """Nouvelle table (fingerprints vides) → success + created=True."""
        structure = {
            'status': 'success',
            'fingerprint_API': 'abc123',
            'fingerprint_UL': 'def456',
            'primary_keys': 'id',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
        }
        table_info = {
            'table_name': 'CSKS',
            'target_schema': 'splus_blue',
            'fingerprint_API': '',
            'fingerprint_UL': '',
        }

        result, config_manager = _run_orchestrator(table_info, structure, {'created': True})

        assert result['status'] == 'success'
        assert result['setup_status'] == 'ready'
        assert result['created'] is True
        assert result['columns_count'] == 1
        assert result['error'] is None
        config_manager.save_setup_result.assert_called_once()

    def test_existing_table_same_fingerprint(self):
        """Table existante, fingerprint identique → success + created=False."""
        fp_api, fp_ul = 'abc123', 'def456'
        structure = {
            'status': 'success',
            'fingerprint_API': fp_api,
            'fingerprint_UL': fp_ul,
            'primary_keys': 'id',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
        }
        table_info = {
            'table_name': 'CSKS',
            'target_schema': 'splus_blue',
            'fingerprint_API': fp_api,
            'fingerprint_UL': fp_ul,
        }

        result, config_manager = _run_orchestrator(table_info, structure, {'created': False})

        assert result['status'] == 'success'
        assert result['created'] is False
        config_manager.save_setup_result.assert_called_once()

    def test_structure_change_returns_blocked(self):
        """Fingerprint changé → blocked, statut sauvegardé."""
        structure = {
            'status': 'success',
            'fingerprint_API': 'NEW_api_fp_NEW_api_fp_',
            'fingerprint_UL': 'NEW_ul_fp_NEW_ul_fp__',
            'primary_keys': 'id',
            'columns': [{'name': 'id', 'type_postgres': 'INTEGER'}],
        }
        table_info = {
            'table_name': 'CSKS',
            'target_schema': 'splus_blue',
            'fingerprint_API': 'OLD_api_fp_OLD_api_fp_',
            'fingerprint_UL': 'OLD_ul_fp_OLD_ul_fp__',
        }

        result, config_manager = _run_orchestrator(table_info, structure)

        assert result['status'] == 'blocked'
        assert result['setup_status'] == 'blocked'
        config_manager.set_setup_status.assert_called_once_with('CSKS', 'blocked')

    def test_api_error_returns_error_result(self):
        """Erreur inattendue → status='error', setup_status='pending'."""
        from amue.services.table_setup_orchestrator import TableSetupOrchestrator

        config_manager = MagicMock()

        with patch('amue.services.table_setup_orchestrator.AMUETableVerifier') as MockVerifier:
            MockVerifier.return_value.verify_structure.side_effect = RuntimeError("API timeout")

            orch = TableSetupOrchestrator(
                api_hook=MagicMock(),
                table_config_manager=config_manager,
            )
            result = orch.run({'table_name': 'CSKS', 'target_schema': 'splus_blue'})

        assert result['status'] == 'error'
        assert result['setup_status'] == 'pending'
        assert 'RuntimeError' in result['error']

    def test_verify_structure_error_status(self):
        """verify_structure retourne status='error' → propagé."""
        structure = {'status': 'error', 'error': 'API unreachable'}
        table_info = {
            'table_name': 'CSKS',
            'target_schema': 'splus_blue',
            'fingerprint_API': '',
            'fingerprint_UL': '',
        }

        result, _ = _run_orchestrator(table_info, structure)

        assert result['status'] == 'error'
        assert result['error'] == 'API unreachable'

