"""
Tests unitaires pour AMUETableVerifier
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestTableVerifierInit:
    """Tests pour l'initialisation de AMUETableVerifier"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_init_success(self, mock_varmgr, mock_create_hook):
        """Initialisation réussie"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        assert verifier.api_hook == mock_api_hook
        assert verifier.endpoint == 'https://api.amue.fr/ul/admin'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_init_no_api_hook(self, mock_varmgr, mock_create_hook):
        """Échec sans api_hook"""
        from amue.operators.table_verifier import AMUETableVerifier

        with pytest.raises(ValueError, match="api_hook est requis"):
            AMUETableVerifier(None)


class TestTableVerifierVerifyStatus:
    """Tests pour verify_status"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_status_ok(self, mock_varmgr, mock_create_hook):
        """Statut OK"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'OK', 'mode': 'FULL'}
        }

        result = verifier.verify_status(table_info)

        assert result['status'] == 'success'
        assert result['status_ok'] is True
        assert result['error'] is None

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_status_ko(self, mock_varmgr, mock_create_hook):
        """Statut KO"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'KO', 'mode': 'FULL'}
        }

        result = verifier.verify_status(table_info)

        assert result['status'] == 'error'
        assert result['status_ok'] is False
        assert 'KO' in result['error']


class TestTableVerifierVerifyStructure:
    """Tests pour verify_structure"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_structure_success(self, mock_varmgr, mock_create_hook):
        """Vérification structure réussie"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)  # Table existe
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        # Structure API
        mock_api_hook.call_api.side_effect = [
            'ID NUMBER(10), NAME VARCHAR2(50)',  # Structure
            'ID'  # Primary keys
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'primary_key': '',
            'needs_pk_update': True,
            'finger_print': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['structure_ok'] is True
        assert len(result['columns']) == 2
        assert result['primary_keys'] == 'ID'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_structure_production_table_missing(self, mock_varmgr, mock_create_hook):
        """En production, table manquante = erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'production'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)  # Table n'existe pas
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID NUMBER(10)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'primary_key': 'ID',
            'needs_pk_update': False,
            'finger_print': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'error'
        assert "n'existe pas en production" in result['error']


class TestTableVerifierVerifyTable:
    """Tests pour verify_table (méthode combinée)"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_table_complete_success(self, mock_varmgr, mock_create_hook):
        """Vérification complète réussie"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID NUMBER(10), NAME VARCHAR2(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': '',
            'needs_pk_update': True,
            'finger_print': ''
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'success'
        assert result['phase'] == 'complete'
        assert 'columns' in result
        assert 'finger_print' in result

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_table_status_error(self, mock_varmgr, mock_create_hook):
        """Erreur à l'étape statut"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'KO'}
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'status'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_verify_table_fingerprint_change(self, mock_varmgr, mock_create_hook):
        """Changement de fingerprint détecté"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID NUMBER(10), NAME VARCHAR2(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'needs_pk_update': False,
            'finger_print': 'old_fingerprint_12345'  # Ancien fingerprint différent
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'CHANGEMENT DE STRUCTURE' in result['error']


class TestTableVerifierFetchStructure:
    """Tests pour _fetch_structure"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_structure_string_response(self, mock_varmgr, mock_create_hook):
        """Parse une réponse string"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 'ID NUMBER(10), NAME VARCHAR2(50), DATE DATE'

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_structure('CSKS')

        assert len(result) == 3
        assert result[0]['name'] == 'ID'
        assert result[0]['type_postgres'] == 'NUMERIC(10)'
        assert result[1]['name'] == 'NAME'
        assert result[1]['type_postgres'] == 'VARCHAR(50)'
        assert result[2]['name'] == 'DATE'
        assert result[2]['type_postgres'] == 'TIMESTAMP'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_structure_empty_raises_error(self, mock_varmgr, mock_create_hook):
        """Structure vide lève une erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ''

        verifier = AMUETableVerifier(mock_api_hook)

        with pytest.raises(ValueError, match="Aucune colonne"):
            verifier._fetch_structure('CSKS')


class TestTableVerifierFetchPrimaryKeys:
    """Tests pour _fetch_primary_keys"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_primary_keys_string(self, mock_varmgr, mock_create_hook):
        """Réponse string"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 'ID,NAME'

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_primary_keys_list(self, mock_varmgr, mock_create_hook):
        """Réponse liste"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ['ID', 'NAME']

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_primary_keys_dict(self, mock_varmgr, mock_create_hook):
        """Réponse dict"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {'keys': ['ID', 'NAME']}

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_fetch_primary_keys_error(self, mock_varmgr, mock_create_hook):
        """Erreur retourne chaîne vide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = Exception("API Error")

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == ''


class TestTableVerifierTableExists:
    """Tests pour _table_exists"""

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_table_exists_true(self, mock_varmgr, mock_create_hook):
        """Table existe"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is True

    @patch('amue.operators.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_verifier.VarMgr')
    def test_table_exists_false(self, mock_varmgr, mock_create_hook):
        """Table n'existe pas"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is False


class TestCheckStructureChange:
    """Tests pour _check_structure_change"""

    def test_no_change(self):
        """Pas de changement"""
        from amue.operators.table_verifier import _check_structure_change

        result = _check_structure_change(
            'CSKS',
            'fingerprint123',
            'fingerprint123',
            True
        )

        assert result is False

    def test_change_detected(self):
        """Changement détecté"""
        from amue.operators.table_verifier import _check_structure_change

        result = _check_structure_change(
            'CSKS',
            'new_fingerprint',
            'old_fingerprint',
            True
        )

        assert result is True

    def test_no_change_if_table_not_exists(self):
        """Pas de changement si table n'existe pas"""
        from amue.operators.table_verifier import _check_structure_change

        result = _check_structure_change(
            'CSKS',
            'new_fingerprint',
            'old_fingerprint',
            False  # Table n'existe pas
        )

        assert result is False

    def test_no_change_if_no_old_fingerprint(self):
        """Pas de changement si pas d'ancien fingerprint"""
        from amue.operators.table_verifier import _check_structure_change

        result = _check_structure_change(
            'CSKS',
            'new_fingerprint',
            '',  # Pas d'ancien fingerprint
            True
        )

        assert result is False
