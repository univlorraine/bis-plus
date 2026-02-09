"""
Tests unitaires pour AMUEStatusChecker
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

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


class TestStatusCheckerFetchFullStatus:
    """Tests pour fetch_full_status (méthode optimisée)"""

    @patch('amue.services.status_checker.VarMgr')
    def test_fetch_full_status_success(self, mock_varmgr):
        """Récupération complète réussie en un seul appel"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'finish': '2024-01-15T10:30:00',
            'status': [
                {'name': 'CSKS', 'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100},
                {'name': 'PRKS', 'status': 'OK', 'mode': 'DELTA', 'count': 500, 'row_size': 50}
            ],
            'nbtables': 2
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.fetch_full_status()

        assert result['http_status'] == 200
        assert result['finish'] == '2024-01-15T10:30:00'
        assert 'CSKS' in result['tables_status']
        assert 'PRKS' in result['tables_status']
        assert result['tables_status']['CSKS']['status'] == 'OK'
        # Un seul appel API
        mock_api_hook.call_api.assert_called_once()

    @patch('amue.services.status_checker.VarMgr')
    def test_fetch_full_status_no_finish(self, mock_varmgr):
        """Récupération sans finish (traitement en cours)"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {
            'status': [{'name': 'CSKS', 'status': 'OK', 'mode': 'FULL', 'count': 1000, 'row_size': 100}]
        }

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.fetch_full_status()

        assert result['http_status'] == 200
        assert result['finish'] is None
        assert 'CSKS' in result['tables_status']

    @patch('amue.services.status_checker.VarMgr')
    def test_fetch_full_status_error(self, mock_varmgr):
        """Gestion des erreurs API"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        error = Exception("Connection error")
        mock_response = MagicMock()
        mock_response.status_code = 503
        error.response = mock_response
        mock_api_hook.call_api.side_effect = error

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.fetch_full_status()

        assert result['http_status'] == 503
        assert result['finish'] is None
        assert result['tables_status'] == {}
        assert 'error' in result

    @patch('amue.services.status_checker.VarMgr')
    def test_fetch_full_status_non_dict_response(self, mock_varmgr):
        """Réponse non-dict retourne valeurs par défaut"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin'
        }.get(key, default)

        from amue.services.status_checker import AMUEStatusChecker

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = "not a dict"

        checker = AMUEStatusChecker(mock_api_hook)
        result = checker.fetch_full_status()

        assert result['http_status'] == 200
        assert result['finish'] is None
        assert result['tables_status'] == {}


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
