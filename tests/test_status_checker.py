"""
Tests unitaires pour AMUEStatusChecker
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestStatusCheckerInit:
    """Tests pour l'initialisation de AMUEStatusChecker"""

    @patch('amue.services.status_checker.VarMgr')
    def test_init_success(self, mock_varmgr):
        """Initialisation réussie avec toutes les variables"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        assert checker.api_hook == mock_api_hook
        assert checker.endpoint == 'https://api.amue.fr/ul/admin'

    @patch('amue.services.status_checker.VarMgr')
    def test_init_missing_universite(self, mock_varmgr):
        """Échec si variable universite manquante"""
        from airflow.exceptions import AirflowException

        mock_varmgr.get.side_effect = KeyError('universite')

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()

        with pytest.raises(AirflowException, match="univ"):
            AMUEStatusChecker(mock_api_hook)

    @patch('amue.services.status_checker.VarMgr')
    def test_init_missing_endpoint(self, mock_varmgr):
        """Échec si variable api_endpoint_admin manquante"""
        from airflow.exceptions import AirflowException

        def get_side_effect(key, default=None):
            if key == 'universite':
                return 'ul'
            raise KeyError('api_endpoint_admin')

        mock_varmgr.get.side_effect = get_side_effect

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()

        with pytest.raises(AirflowException, match="api_endpoint_admin"):
            AMUEStatusChecker(mock_api_hook)


class TestStatusCheckerGetCurrentStatus:
    """Tests pour get_current_status"""

    @patch('amue.services.status_checker.VarMgr')
    def test_get_current_status_success(self, mock_varmgr):
        """Récupération réussie du statut actuel"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'status': [
                {'name': 'CSKS', 'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100},
                {'name': 'PRKS', 'status': 'OK', 'mode': 'DELTA', 'count': 500, 'row_size': 50}
            ]
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.get_current_status()

        assert 'CSKS' in result
        assert 'PRKS' in result
        assert result['CSKS']['status'] == 'OK'
        assert result['CSKS']['mode'] == 'FULL'
        assert result['CSKS']['count'] == 1000

    @patch('amue.services.status_checker.VarMgr')
    def test_get_current_status_invalid_response(self, mock_varmgr):
        """Réponse invalide lève une erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {'invalid': 'response'}

        checker = AMUEStatusChecker(mock_api_hook)

        with pytest.raises(ValueError, match="Format réponse invalide"):
            checker.get_current_status()


class TestStatusCheckerCheckStatusCode:
    """Tests pour check_status_code"""

    @patch('amue.services.status_checker.VarMgr')
    def test_check_status_code(self, mock_varmgr):
        """Vérifie uniquement le code HTTP"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 200

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.check_status_code()

        assert result == 200
        mock_api_hook.call_api.assert_called_once()
        # Vérifie que check_status_only=True est passé
        call_args = mock_api_hook.call_api.call_args
        assert call_args[1]['check_status_only'] is True


class TestStatusCheckerCheckFinishStatus:
    """Tests pour check_finish_status"""

    @patch('amue.services.status_checker.VarMgr')
    def test_check_finish_status_present(self, mock_varmgr):
        """Variable finish présente"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'finish': '2024-01-15T10:30:00',
            'status': []
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.check_finish_status()

        assert result == '2024-01-15T10:30:00'

    @patch('amue.services.status_checker.VarMgr')
    def test_check_finish_status_absent(self, mock_varmgr):
        """Variable finish absente (traitement en cours)"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'status': []
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.check_finish_status()

        assert result is None

    @patch('amue.services.status_checker.VarMgr')
    def test_check_finish_status_non_dict_response(self, mock_varmgr):
        """Réponse non-dict retourne None"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = "not a dict"

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.check_finish_status()

        assert result is None


class TestStatusCheckerParseTablesStatus:
    """Tests pour _parse_tables_status"""

    @patch('amue.services.status_checker.VarMgr')
    def test_parse_tables_status_valid(self, mock_varmgr):
        """Parse une liste de statuts valide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        status_list = [
            {'name': 'csks', 'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100},
            {'name': 'prks', 'status': 'KO', 'mode': 'DELTA', 'count': 0, 'row_size': 0}
        ]

        result = checker._parse_tables_status(status_list)

        assert 'CSKS' in result  # Nom converti en majuscules
        assert 'PRKS' in result
        assert result['CSKS']['status'] == 'OK'
        assert result['PRKS']['status'] == 'KO'

    @patch('amue.services.status_checker.VarMgr')
    def test_parse_tables_status_empty_list(self, mock_varmgr):
        """Liste vide retourne dict vide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        result = checker._parse_tables_status([])

        assert result == {}

    @patch('amue.services.status_checker.VarMgr')
    def test_parse_tables_status_non_list(self, mock_varmgr):
        """Non-list retourne dict vide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        result = checker._parse_tables_status("not a list")

        assert result == {}

    @patch('amue.services.status_checker.VarMgr')
    def test_parse_tables_status_missing_name(self, mock_varmgr):
        """Entrées sans nom sont ignorées"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        status_list = [
            {'name': 'CSKS', 'status': 'OK'},
            {'status': 'OK'},  # Pas de nom
            {'name': '', 'status': 'OK'},  # Nom vide
        ]

        result = checker._parse_tables_status(status_list)

        assert len(result) == 1
        assert 'CSKS' in result


class TestStatusCheckerHistoricalStatus:
    """Tests pour check_historical_status"""

    @patch('amue.services.status_checker.VarMgr')
    def test_check_historical_status(self, mock_varmgr):
        """Vérification historique sur plusieurs jours"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'amue_last_successful_run': ''
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'status': [{'name': 'CSKS', 'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}],
            'finish': '2024-01-15',
            'nbtables': 1,
            'nbtables_ko': 0
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.check_historical_status(max_days=3)

        assert 'status_by_date' in result
        assert 'dates_checked' in result


class TestStatusCheckerHelperMethods:
    """Tests pour les méthodes utilitaires"""

    @patch('amue.services.status_checker.VarMgr')
    def test_get_last_success_date_valid(self, mock_varmgr):
        """Récupère une date de dernier succès valide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'amue_last_successful_run': '2024-01-10'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        result = checker._get_last_success_date()

        assert result == datetime(2024, 1, 10).date()

    @patch('amue.services.status_checker.VarMgr')
    def test_get_last_success_date_invalid(self, mock_varmgr):
        """Date invalide retourne hier"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'amue_last_successful_run': 'invalid-date'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        result = checker._get_last_success_date()
        expected = (datetime.now() - timedelta(days=1)).date()

        assert result == expected

    @patch('amue.services.status_checker.VarMgr')
    def test_compute_days_to_check(self, mock_varmgr):
        """Calcul des jours à vérifier"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        today = datetime.now().date()
        last_success = today - timedelta(days=3)

        result = checker._compute_days_to_check(last_success, max_days=7)

        # Devrait inclure aujourd'hui et les 2 jours précédents (jusqu'à last_success exclus)
        assert len(result) == 3
        assert today in result

    @patch('amue.services.status_checker.VarMgr')
    def test_serialize_dates(self, mock_varmgr):
        """Sérialisation des dates pour JSON"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        checker = AMUEStatusChecker(mock_api_hook)

        status_by_date = {
            '20240115': {
                'date': datetime(2024, 1, 15).date(),
                'tables_status': {'CSKS': {'status': 'OK'}}
            }
        }

        result = checker._serialize_dates(status_by_date)

        assert result['20240115']['date'] == '2024-01-15'
