"""
Tests unitaires pour AMUETableVerifier
"""
import pytest
from unittest.mock import MagicMock, patch


class TestTableVerifierInit:
    """Tests pour l'initialisation de AMUETableVerifier"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_init_success(self, mock_varmgr, mock_create_hook):
        """Initialisation réussie"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        assert verifier.api_hook == mock_api_hook
        assert verifier.endpoint == 'https://api.amue.fr/ul/admin'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_init_no_api_hook(self, mock_varmgr, mock_create_hook):
        """Échec sans api_hook"""
        from amue.operators.table_management.table_verifier import AMUETableVerifier

        with pytest.raises(ValueError, match="api_hook est requis"):
            AMUETableVerifier(None)


class TestTableVerifierVerifyStatus:
    """Tests pour verify_status"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_status_ok(self, mock_varmgr, mock_create_hook):
        """Statut OK"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK', 'mode': 'FULL'}
        }

        result = verifier.verify_status(table_info)

        assert result['status'] == 'success'
        assert result['status_ok'] is True
        assert result['error'] is None

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_status_ko(self, mock_varmgr, mock_create_hook):
        """Statut KO"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'KO', 'mode': 'FULL'}
        }

        result = verifier.verify_status(table_info)

        assert result['status'] == 'error'
        assert result['status_ok'] is False
        assert 'KO' in result['error']


class TestTableVerifierVerifyStructure:
    """Tests pour verify_structure"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_success_fetch_pk_from_api(self, mock_varmgr, mock_create_hook):
        """Vérification structure réussie - PK absente, récupérée depuis l'API"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)  # Table existe
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        # Structure API + Primary keys (toujours fetchées)
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',  # Structure
            'ID'  # Primary keys
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['structure_ok'] is True
        assert len(result['columns']) == 2
        assert result['primary_keys'] == 'ID'
        assert 'fingerprint_API' in result
        assert 'fingerprint_local' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_keeps_existing_pk(self, mock_varmgr, mock_create_hook):
        """PK existante dans la variable Airflow est conservée, mais API PKs toujours fetchées"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        # Structure + PKs (toujours fetchées maintenant)
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',  # Structure
            'ID'  # Primary keys API
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'BUKRS,KOSTL',  # PK déjà définie en config
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['primary_keys'] == 'BUKRS,KOSTL'  # Config PKs prioritaires
        # Deux appels API : structure + PKs (toujours fetchées)
        assert mock_api_hook.call_api.call_count == 2

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_table_missing_returns_success(self, mock_varmgr, mock_create_hook):
        """Table manquante retourne success avec exists=False (sera creee par table_manager)"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'production',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)  # Table n'existe pas
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'ID',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['exists'] is False


class TestTableVerifierVerifyTable:
    """Tests pour verify_table (méthode combinée)"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_complete_success(self, mock_varmgr, mock_create_hook):
        """Vérification complète réussie"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'success'
        assert result['phase'] == 'complete'
        assert 'columns' in result
        assert 'fingerprint_API' in result
        assert 'fingerprint_local' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_status_error(self, mock_varmgr, mock_create_hook):
        """Erreur à l'étape statut"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'KO'}
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'status'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_fingerprint_change(self, mock_varmgr, mock_create_hook):
        """Changement de fingerprint détecté (les deux changent)"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': 'old_api_fingerprint_12345',
            'fingerprint_local': 'old_ul_fingerprint_12345'
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'CHANGEMENT DE STRUCTURE' in result['error']
        assert 'fingerprint_API' in result['error']
        assert 'fingerprint_local' in result['error']

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_only_api_fingerprint_changed(self, mock_varmgr, mock_create_hook):
        """Seul fingerprint_API change"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from common.utils.fingerprint import compute_structure_hash_with_pk

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        # Calcule le bon fingerprint_local pour qu'il ne change pas
        columns = [
            {'name': 'ID', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        correct_fp_local = compute_structure_hash_with_pk(columns, 'ID', type_key='type_postgres')

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': 'old_api_fingerprint_12345',  # Différent -> changé
            'fingerprint_local': correct_fp_local  # Identique -> pas changé
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'fingerprint_API' in result['error']
        assert 'fingerprint_local' not in result['error']

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_only_ul_fingerprint_changed(self, mock_varmgr, mock_create_hook):
        """Seul fingerprint_local change"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from common.utils.fingerprint import compute_structure_hash_with_pk

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        # Calcule le bon fingerprint_API pour qu'il ne change pas
        columns = [
            {'name': 'ID', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        correct_fp_api = compute_structure_hash_with_pk(columns, 'ID', type_key='type_original')

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': correct_fp_api,  # Identique -> pas changé
            'fingerprint_local': 'old_ul_fingerprint_12345'  # Différent -> changé
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'fingerprint_local' in result['error']
        assert 'fingerprint_API' not in result['error']


class TestTableVerifierFetchStructure:
    """Tests pour _fetch_structure"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_structure_string_response(self, mock_varmgr, mock_create_hook):
        """Parse une réponse string"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 'ID INTEGER(10), NAME VARCHAR(50), DATE DATE'

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_structure('CSKS')

        assert len(result) == 3
        assert result[0]['name'] == 'ID'
        assert result[0]['type_postgres'] == 'BIGINT'
        assert result[1]['name'] == 'NAME'
        assert result[1]['type_postgres'] == 'VARCHAR(50)'
        assert result[2]['name'] == 'DATE'
        assert result[2]['type_postgres'] == 'TIMESTAMP'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_structure_empty_raises_error(self, mock_varmgr, mock_create_hook):
        """Structure vide lève une erreur"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ''

        verifier = AMUETableVerifier(mock_api_hook)

        with pytest.raises(ValueError, match="Aucune colonne"):
            verifier._fetch_structure('CSKS')


class TestTableVerifierFetchPrimaryKeys:
    """Tests pour _fetch_primary_keys"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_string(self, mock_varmgr, mock_create_hook):
        """Réponse string"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 'ID,NAME'

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_list(self, mock_varmgr, mock_create_hook):
        """Réponse liste"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ['ID', 'NAME']

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_dict(self, mock_varmgr, mock_create_hook):
        """Réponse dict"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {'keys': ['ID', 'NAME']}

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_error(self, mock_varmgr, mock_create_hook):
        """Erreur retourne chaîne vide"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = Exception("API Error")

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == ''


class TestTableVerifierTableExists:
    """Tests pour _table_exists"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_table_exists_true(self, mock_varmgr, mock_create_hook):
        """Table existe"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is True

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_table_exists_false(self, mock_varmgr, mock_create_hook):
        """Table n'existe pas"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is False


class TestTableVerifierSavePrimaryKeys:
    """Tests pour _save_primary_keys (persistance immédiate en BDD)"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_save_primary_keys_success(self, mock_tcm_cls, mock_varmgr, mock_create_hook):
        """Sauvegarde des PKs via TableConfigManager"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        verifier._save_primary_keys('CSKS', 'ID,NAME')

        mock_tcm.save_primary_keys.assert_called_once_with('CSKS', 'ID,NAME')

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_save_primary_keys_empty_aborts(self, mock_varmgr, mock_create_hook):
        """Abandon si PKs vides — pas d'appel à TableConfigManager"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        with patch('amue.services.table_config_manager.TableConfigManager') as mock_tcm_cls:
            verifier._save_primary_keys('CSKS', '')
            mock_tcm_cls.assert_not_called()

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_verify_structure_calls_save_pks(self, mock_tcm_cls, mock_varmgr, mock_create_hook):
        """verify_structure appelle _save_primary_keys après récupération API"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',  # Structure
            'ID'  # Primary keys
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        # Vérifie que les PKs ont été sauvegardées via TableConfigManager
        assert result['primary_keys'] == 'ID'
        mock_tcm.save_primary_keys.assert_called_once_with('CSKS', 'ID')

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_always_fetches_api_pks(self, mock_varmgr, mock_create_hook):
        """verify_structure appelle toujours l'API pour les PKs même si config_pks existe"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',  # Structure
            'ID'  # Primary keys API
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'BUKRS,KOSTL',  # Config PKs déjà définies
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        verifier.verify_structure(table_info)

        # 2 appels API : structure + PKs (toujours)
        assert mock_api_hook.call_api.call_count == 2


class TestCheckFingerprintChanges:
    """Tests pour _check_fingerprint_changes"""

    def test_no_change(self):
        """Pas de changement"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'fp_api_123', 'fp_api_123',
            'fp_ul_456', 'fp_ul_456',
            True
        )

        assert result['api_changed'] is False
        assert result['ul_changed'] is False

    def test_api_change_detected(self):
        """Changement fingerprint_API détecté"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'new_api_fp', 'old_api_fp',
            'same_ul_fp', 'same_ul_fp',
            True
        )

        assert result['api_changed'] is True
        assert result['ul_changed'] is False

    def test_ul_change_detected(self):
        """Changement fingerprint_local détecté"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'same_api_fp', 'same_api_fp',
            'new_ul_fp', 'old_ul_fp',
            True
        )

        assert result['api_changed'] is False
        assert result['ul_changed'] is True

    def test_both_changed(self):
        """Les deux fingerprints changent"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'new_api_fp', 'old_api_fp',
            'new_ul_fp', 'old_ul_fp',
            True
        )

        assert result['api_changed'] is True
        assert result['ul_changed'] is True

    def test_no_change_if_table_not_exists(self):
        """Pas de changement si table n'existe pas"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'new_api_fp', 'old_api_fp',
            'new_ul_fp', 'old_ul_fp',
            False  # Table n'existe pas
        )

        assert result['api_changed'] is False
        assert result['ul_changed'] is False

    def test_no_change_if_no_old_fingerprint(self):
        """Pas de changement si pas d'ancien fingerprint"""
        from amue.operators.table_management.table_verifier import _check_fingerprint_changes

        result = _check_fingerprint_changes(
            'CSKS',
            'new_api_fp', '',
            'new_ul_fp', '',
            True
        )

        assert result['api_changed'] is False
        assert result['ul_changed'] is False


class TestFormatPgType:
    """Tests pour _format_pg_type"""

    def test_varchar_with_length(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('character varying', 50, None, None) == 'VARCHAR(50)'

    def test_varchar_without_length(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('character varying', None, None, None) == 'VARCHAR'

    def test_bpchar_with_length(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('character', 10, None, None) == 'BPCHAR(10)'

    def test_bpchar_without_length(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('character', None, None, None) == 'BPCHAR'

    def test_numeric_with_precision_and_scale(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('numeric', None, 10, 2) == 'NUMERIC(10,2)'

    def test_numeric_with_precision_only(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('numeric', None, 10, None) == 'NUMERIC(10)'

    def test_numeric_without_precision(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('numeric', None, None, None) == 'NUMERIC'

    def test_timestamp_without_tz(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('timestamp without time zone', None, None, None) == 'TIMESTAMP'

    def test_timestamp_with_tz(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('timestamp with time zone', None, None, None) == 'TIMESTAMPTZ'

    def test_double_precision(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('double precision', None, None, None) == 'DOUBLE PRECISION'

    def test_integer(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('integer', None, None, None) == 'INTEGER'

    def test_bigint(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('bigint', None, None, None) == 'BIGINT'

    def test_text(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('text', None, None, None) == 'TEXT'

    def test_smallint(self):
        from amue.operators.table_management.table_verifier import AMUETableVerifier
        assert AMUETableVerifier._format_pg_type('smallint', None, None, None) == 'SMALLINT'


class TestComputeStructureDiff:
    """Tests pour _compute_structure_diff"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_added_column(self, mock_varmgr, mock_create_hook):
        """Colonne ajoutée détectée"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        # Existing columns in DB: only ID
        mock_postgres_hook.get_records.return_value = [
            ('id', 'bigint', None, None, None),
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        new_columns = [
            {'name': 'ID', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_postgres': 'VARCHAR(50)'},
        ]

        result = verifier._compute_structure_diff('CSKS', new_columns)

        assert '+ NAME (VARCHAR(50))' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_removed_column(self, mock_varmgr, mock_create_hook):
        """Colonne supprimée détectée"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        # Existing columns: ID and OLD_COL
        mock_postgres_hook.get_records.return_value = [
            ('id', 'bigint', None, None, None),
            ('old_col', 'text', None, None, None),
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        new_columns = [
            {'name': 'ID', 'type_postgres': 'BIGINT'},
        ]

        result = verifier._compute_structure_diff('CSKS', new_columns)

        assert '- OLD_COL (TEXT)' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_type_changed(self, mock_varmgr, mock_create_hook):
        """Changement de type détecté"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('id', 'integer', None, None, None),
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        new_columns = [
            {'name': 'ID', 'type_postgres': 'BIGINT'},
        ]

        result = verifier._compute_structure_diff('CSKS', new_columns)

        assert '~ ID: INTEGER -> BIGINT' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_pk_only_change(self, mock_varmgr, mock_create_hook):
        """Aucune différence de colonnes -> changement de PKs probable"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('id', 'bigint', None, None, None),
            ('name', 'character varying', 50, None, None),
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        new_columns = [
            {'name': 'ID', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_postgres': 'VARCHAR(50)'},
        ]

        result = verifier._compute_structure_diff('CSKS', new_columns)

        assert 'cles primaires' in result

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_diff_fetch_error(self, mock_varmgr, mock_create_hook):
        """Erreur lors du fetch des colonnes existantes"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.side_effect = Exception("Connection lost")
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._compute_structure_diff('CSKS', [])

        assert 'impossible de calculer le diff' in result


class TestFingerprintErrorIncludesDiff:
    """Tests pour le diff dans les erreurs de fingerprint"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fingerprint_api_error_includes_cause(self, mock_varmgr, mock_create_hook):
        """Erreur fingerprint_API inclut la cause AMUE"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from common.utils.fingerprint import compute_structure_hash_with_pk

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        # Compute correct UL fingerprint so only API changes
        columns = [
            {'name': 'ID', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        correct_fp_local = compute_structure_hash_with_pk(columns, 'ID', type_key='type_postgres')

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': 'old_api_fingerprint_12345',
            'fingerprint_local': correct_fp_local
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert 'AMUE a modifie la structure source' in result['error']

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fingerprint_local_error_includes_diff(self, mock_varmgr, mock_create_hook):
        """Erreur fingerprint_local inclut le diff structurel"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        # _compute_structure_diff will call get_records
        mock_postgres_hook.get_records.return_value = [
            ('id', 'integer', None, None, None),  # Different type than new BIGINT
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from common.utils.fingerprint import compute_structure_hash_with_pk

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        # Compute correct API fingerprint so only UL changes
        columns = [
            {'name': 'ID', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        correct_fp_api = compute_structure_hash_with_pk(columns, 'ID', type_key='type_original')

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': correct_fp_api,
            'fingerprint_local': 'old_ul_fingerprint_12345'
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert 'Differences:' in result['error'] or 'cles primaires' in result['error']


class TestVerifyStatusErrorDetails:
    """Tests pour les détails dans verify_status"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_status_error_includes_details(self, mock_varmgr, mock_create_hook):
        """Le message d'erreur inclut les détails du statut"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'current_status': {'status': 'KO', 'mode': 'FULL', 'message': 'erreur sync'}
        }

        result = verifier.verify_status(table_info)

        assert result['status'] == 'error'
        assert 'Details:' in result['error']
        assert 'erreur sync' in result['error']
        assert 'FULL' in result['error']


class TestVerifyStructureErrorMessages:
    """Tests pour les messages d'erreur enrichis de verify_structure"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_table_missing_returns_success_with_exists_false(self, mock_varmgr, mock_create_hook):
        """Table manquante retourne success avec exists=False"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'production',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (False,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook, target_schema='splus_blue')

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'ID',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['exists'] is False

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_generic_error_includes_exception_type(self, mock_varmgr, mock_create_hook):
        """Le catch générique inclut le type d'exception"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = ConnectionError("timeout")

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'ID',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'error'
        assert '[ConnectionError]' in result['error']


class TestTableVerifierVerifyStructureException:
    """Tests pour la gestion d'exceptions dans verify_structure"""

    @patch('amue.operators.table_management.table_verifier.resolve_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_exception_returns_error_dict(self, mock_varmgr, mock_create_hook):
        """Exception dans _fetch_structure → retourne dict {status: 'error'} sans lever AttributeError"""
        _vars = {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
        }
        mock_varmgr.get.side_effect = lambda key, default=None: _vars.get(key, default)
        mock_varmgr.get_required.side_effect = lambda key, error_msg=None: _vars[key]

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'table_name': 'CSKS',
            'primary_key': 'ID',
            'fingerprint_API': '',
            'fingerprint_local': ''
        }

        with patch.object(verifier, '_fetch_structure', side_effect=RuntimeError("test error")):
            result = verifier.verify_structure(table_info)

        assert result['status'] == 'error'
        assert 'test error' in result['error']
