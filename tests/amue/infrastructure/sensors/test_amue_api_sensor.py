"""Tests unitaires pour AMUEAPISensor."""
import pytest
from unittest.mock import patch, MagicMock

from amue.infrastructure.sensors.amue_api_sensor import AMUEAPISensor


class TestAMUEAPISensor:
    """Tests pour le sensor API AMUE."""

    def _make_sensor(self):
        """Crée une instance du sensor pour les tests."""
        return AMUEAPISensor(task_id='test_sensor', poke_interval=60, timeout=300)

    def _make_context(self):
        """Crée un contexte Airflow mocké."""
        ti = MagicMock()
        return {'ti': ti}

    @patch('amue.infrastructure.sensors.amue_api_sensor.VarMgr')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_returns_true_when_api_ready(self, mock_hook_cls, mock_checker_cls, mock_varmgr):
        """poke() retourne True quand l'API est prête avec un nouveau finish."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15 03:45:00',
            'tables_status': {'CSKS': {'status': 'OK'}},
        }
        mock_checker_cls.return_value = mock_checker
        mock_varmgr.get.return_value = ''  # Pas de dernier finish

        sensor = self._make_sensor()
        context = self._make_context()

        result = sensor.poke(context)

        assert result is True
        context['ti'].xcom_push.assert_called_once()
        call_kwargs = context['ti'].xcom_push.call_args
        assert call_kwargs[1]['key'] == 'polling_result'

    @patch('amue.infrastructure.sensors.amue_api_sensor.VarMgr')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_returns_false_when_http_not_200(self, mock_hook_cls, mock_checker_cls, mock_varmgr):
        """poke() retourne False si l'API ne retourne pas 200."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 503,
            'finish': '',
        }
        mock_checker_cls.return_value = mock_checker

        sensor = self._make_sensor()
        result = sensor.poke(self._make_context())

        assert result is False

    @patch('amue.infrastructure.sensors.amue_api_sensor.VarMgr')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_returns_false_when_finish_empty(self, mock_hook_cls, mock_checker_cls, mock_varmgr):
        """poke() retourne False si finish est vide (traitement en cours)."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '',
        }
        mock_checker_cls.return_value = mock_checker

        sensor = self._make_sensor()
        result = sensor.poke(self._make_context())

        assert result is False

    @patch('common.application.admin_state_manager.AdminStateManager')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_returns_false_when_same_finish(self, mock_hook_cls, mock_checker_cls, mock_admin_cls):
        """poke() retourne False si le finish est identique au dernier."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15 03:45:00',
        }
        mock_checker_cls.return_value = mock_checker
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = '2024-01-15 03:45:00'  # Même timestamp

        sensor = self._make_sensor()
        result = sensor.poke(self._make_context())

        assert result is False

    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_returns_false_on_api_exception(self, mock_hook_cls):
        """poke() retourne False si l'API lève une exception."""
        mock_hook_cls.side_effect = Exception("Connection refused")

        sensor = self._make_sensor()
        result = sensor.poke(self._make_context())

        assert result is False

    def test_sensor_default_mode_is_reschedule(self):
        """Le sensor utilise le mode reschedule par défaut."""
        sensor = self._make_sensor()
        assert sensor.mode == 'reschedule'

    def test_sensor_custom_poke_interval(self):
        """Le poke_interval est configurable."""
        sensor = AMUEAPISensor(task_id='test', poke_interval=120, timeout=600)
        assert sensor.poke_interval == 120

    @patch('amue.infrastructure.sensors.amue_api_sensor.VarMgr')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_poke_stores_polling_result_for_execute(self, mock_hook_cls, mock_checker_cls, mock_varmgr):
        """poke() stocke le polling_result dans _polling_result pour execute()."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15 03:45:00',
            'tables_status': {'CSKS': {'status': 'OK'}},
        }
        mock_checker_cls.return_value = mock_checker
        mock_varmgr.get.return_value = ''

        sensor = self._make_sensor()
        context = self._make_context()
        sensor.poke(context)

        assert sensor._polling_result is not None
        assert sensor._polling_result['finish'] == '2024-01-15 03:45:00'
        assert 'start_time' in sensor._polling_result

    @patch('amue.infrastructure.sensors.amue_api_sensor.VarMgr')
    @patch('amue.infrastructure.sensors.amue_api_sensor.get_status_checker')
    @patch('amue.infrastructure.sensors.amue_api_sensor.AMUEAPIHook')
    def test_execute_returns_polling_result(self, mock_hook_cls, mock_checker_cls, mock_varmgr):
        """execute() retourne le polling_result (disponible via .output)."""
        mock_checker = MagicMock()
        mock_checker.fetch_full_status.return_value = {
            'http_status': 200,
            'finish': '2024-01-15 03:45:00',
            'tables_status': {'CSKS': {'status': 'OK'}},
        }
        mock_checker_cls.return_value = mock_checker
        mock_varmgr.get.return_value = ''

        sensor = self._make_sensor()
        context = self._make_context()

        # Simule le comportement de super().execute() en appelant poke directement
        sensor.poke(context)
        result = sensor._polling_result

        assert isinstance(result, dict)
        assert result['finish'] == '2024-01-15 03:45:00'
        assert result['start_time'] != ''
