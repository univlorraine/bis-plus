"""
Tests unitaires pour AMUETableVerifier
"""
import pytest
from unittest.mock import MagicMock, patch


class TestTableVerifierInit:
    """Tests pour l'initialisation de AMUETableVerifier"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_init_success(self, mock_varmgr, mock_create_hook):
        """Initialisation réussie"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        assert verifier.api_hook == mock_api_hook
        assert verifier.endpoint == 'https://api.amue.fr/ul/admin'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_init_no_api_hook(self, mock_varmgr, mock_create_hook):
        """Échec sans api_hook"""
        from amue.operators.table_management.table_verifier import AMUETableVerifier

        with pytest.raises(ValueError, match="api_hook est requis"):
            AMUETableVerifier(None)


class TestTableVerifierVerifyStatus:
    """Tests pour verify_status"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_status_ok(self, mock_varmgr, mock_create_hook):
        """Statut OK"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

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

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_status_ko(self, mock_varmgr, mock_create_hook):
        """Statut KO"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

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

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_success_fetch_pk_from_api(self, mock_varmgr, mock_create_hook):
        """Vérification structure réussie - PK absente, récupérée depuis l'API"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

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
            'name': 'CSKS',
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_UL': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['structure_ok'] is True
        assert len(result['columns']) == 2
        assert result['primary_keys'] == 'ID'
        assert 'fingerprint_API' in result
        assert 'fingerprint_UL' in result

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_keeps_existing_pk(self, mock_varmgr, mock_create_hook):
        """PK existante dans la variable Airflow est conservée, mais API PKs toujours fetchées"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

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
            'name': 'CSKS',
            'primary_key': 'BUKRS,KOSTL',  # PK déjà définie en config
            'fingerprint_API': '',
            'fingerprint_UL': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'success'
        assert result['primary_keys'] == 'BUKRS,KOSTL'  # Config PKs prioritaires
        # Deux appels API : structure + PKs (toujours fetchées)
        assert mock_api_hook.call_api.call_count == 2

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
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

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'primary_key': 'ID',
            'fingerprint_API': '',
            'fingerprint_UL': ''
        }

        result = verifier.verify_structure(table_info)

        assert result['status'] == 'error'
        assert "n'existe pas en production" in result['error']


class TestTableVerifierVerifyTable:
    """Tests pour verify_table (méthode combinée)"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
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

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_UL': ''
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'success'
        assert result['phase'] == 'complete'
        assert 'columns' in result
        assert 'fingerprint_API' in result
        assert 'fingerprint_UL' in result

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_status_error(self, mock_varmgr, mock_create_hook):
        """Erreur à l'étape statut"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'KO'}
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'status'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_fingerprint_change(self, mock_varmgr, mock_create_hook):
        """Changement de fingerprint détecté (les deux changent)"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

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
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': 'old_api_fingerprint_12345',
            'fingerprint_UL': 'old_ul_fingerprint_12345'
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'CHANGEMENT DE STRUCTURE' in result['error']
        assert 'fingerprint_API' in result['error']
        assert 'fingerprint_UL' in result['error']

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_only_api_fingerprint_changed(self, mock_varmgr, mock_create_hook):
        """Seul fingerprint_API change"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from amue.utils.transformers import compute_structure_hash_with_pk

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = [
            'ID INTEGER(10), NAME VARCHAR(50)',
            'ID'
        ]

        verifier = AMUETableVerifier(mock_api_hook)

        # Calcule le bon fingerprint_UL pour qu'il ne change pas
        columns = [
            {'name': 'ID', 'type_original': 'INTEGER(10)', 'type_postgres': 'BIGINT'},
            {'name': 'NAME', 'type_original': 'VARCHAR(50)', 'type_postgres': 'VARCHAR(50)'}
        ]
        correct_fp_ul = compute_structure_hash_with_pk(columns, 'ID', type_key='type_postgres')

        table_info = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': 'old_api_fingerprint_12345',  # Différent -> changé
            'fingerprint_UL': correct_fp_ul  # Identique -> pas changé
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'fingerprint_API' in result['error']
        assert 'fingerprint_UL' not in result['error']

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_table_only_ul_fingerprint_changed(self, mock_varmgr, mock_create_hook):
        """Seul fingerprint_UL change"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = (True,)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.operators.table_management.table_verifier import AMUETableVerifier
        from amue.utils.transformers import compute_structure_hash_with_pk

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
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'primary_key': 'ID',
            'fingerprint_API': correct_fp_api,  # Identique -> pas changé
            'fingerprint_UL': 'old_ul_fingerprint_12345'  # Différent -> changé
        }

        result = verifier.verify_table(table_info)

        assert result['status'] == 'error'
        assert result['phase'] == 'fingerprint'
        assert 'fingerprint_UL' in result['error']
        assert 'fingerprint_API' not in result['error']


class TestTableVerifierFetchStructure:
    """Tests pour _fetch_structure"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_structure_string_response(self, mock_varmgr, mock_create_hook):
        """Parse une réponse string"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

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

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_structure_empty_raises_error(self, mock_varmgr, mock_create_hook):
        """Structure vide lève une erreur"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ''

        verifier = AMUETableVerifier(mock_api_hook)

        with pytest.raises(ValueError, match="Aucune colonne"):
            verifier._fetch_structure('CSKS')


class TestTableVerifierFetchPrimaryKeys:
    """Tests pour _fetch_primary_keys"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_string(self, mock_varmgr, mock_create_hook):
        """Réponse string"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = 'ID,NAME'

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_list(self, mock_varmgr, mock_create_hook):
        """Réponse liste"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = ['ID', 'NAME']

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_dict(self, mock_varmgr, mock_create_hook):
        """Réponse dict"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.return_value = {'keys': ['ID', 'NAME']}

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_fetch_primary_keys_error(self, mock_varmgr, mock_create_hook):
        """Erreur retourne chaîne vide"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        mock_api_hook.call_api.side_effect = Exception("API Error")

        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._fetch_primary_keys('CSKS')

        assert result == ''


class TestTableVerifierTableExists:
    """Tests pour _table_exists"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
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

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is True

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
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

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        result = verifier._table_exists('CSKS')

        assert result is False


class TestTableVerifierSavePrimaryKeys:
    """Tests pour _save_primary_keys (persistance immédiate)"""

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_save_primary_keys_success(self, mock_varmgr, mock_create_hook):
        """Sauvegarde des PKs dans la variable Airflow"""
        import json

        tables_config = json.dumps([
            {'name': 'CSKS', 'primary_key': '', 'fingerprint_API': '', 'fingerprint_UL': ''}
        ])

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
            'amue_tables_to_import': tables_config
        }.get(key, default)
        mock_varmgr.set.return_value = True

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        verifier._save_primary_keys('CSKS', 'ID,NAME')

        # Vérifie que set a été appelé avec les PKs mises à jour
        mock_varmgr.set.assert_called()
        call_args = mock_varmgr.set.call_args[0]
        saved_config = json.loads(call_args[1])
        assert saved_config[0]['primary_key'] == 'ID,NAME'

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_save_primary_keys_no_change(self, mock_varmgr, mock_create_hook):
        """Pas de sauvegarde si PKs identiques"""
        import json

        tables_config = json.dumps([
            {'name': 'CSKS', 'primary_key': 'ID,NAME', 'fingerprint_API': '', 'fingerprint_UL': ''}
        ])

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
            'amue_tables_to_import': tables_config
        }.get(key, default)

        from amue.operators.table_management.table_verifier import AMUETableVerifier

        mock_api_hook = MagicMock()
        verifier = AMUETableVerifier(mock_api_hook)

        verifier._save_primary_keys('CSKS', 'ID,NAME')

        # Pas d'appel à set car les PKs sont identiques
        mock_varmgr.set.assert_not_called()

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_calls_save_pks(self, mock_varmgr, mock_create_hook):
        """verify_structure appelle _save_primary_keys après récupération API"""
        import json

        tables_config = json.dumps([
            {'name': 'CSKS', 'primary_key': '', 'fingerprint_API': '', 'fingerprint_UL': ''}
        ])

        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev',
            'amue_tables_to_import': tables_config
        }.get(key, default)
        mock_varmgr.set.return_value = True

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
            'name': 'CSKS',
            'primary_key': '',
            'fingerprint_API': '',
            'fingerprint_UL': ''
        }

        result = verifier.verify_structure(table_info)

        # Vérifie que les PKs ont été sauvegardées
        assert result['primary_keys'] == 'ID'
        mock_varmgr.set.assert_called()

    @patch('amue.operators.table_management.table_verifier.create_postgres_hook')
    @patch('amue.operators.table_management.table_verifier.VarMgr')
    def test_verify_structure_always_fetches_api_pks(self, mock_varmgr, mock_create_hook):
        """verify_structure appelle toujours l'API pour les PKs même si config_pks existe"""
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'universite': 'ul',
            'api_endpoint_admin': 'https://api.amue.fr/${univ}/admin',
            'environment': 'dev'
        }.get(key, default)

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
            'name': 'CSKS',
            'primary_key': 'BUKRS,KOSTL',  # Config PKs déjà définies
            'fingerprint_API': '',
            'fingerprint_UL': ''
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
        """Changement fingerprint_UL détecté"""
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
