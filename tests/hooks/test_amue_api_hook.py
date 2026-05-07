"""
Tests unitaires pour AMUEAPIHook
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


class TestAMUEAPIHookTokenExpiration:
    """Tests pour la gestion d'expiration du token OAuth"""

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_is_token_expired_no_token(self, mock_get_conn):
        """Sans token, _is_token_expired doit retourner True"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        assert hook._is_token_expired() is True

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_is_token_expired_token_still_valid(self, mock_get_conn):
        """Token avec expiration future doit retourner False"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook, _token_cache

        # Reset le cache et ajoute un token valide
        _token_cache.invalidate()
        _token_cache.set_token('valid_token', 3600)  # 1 heure

        hook = AMUEAPIHook()

        assert hook._is_token_expired() is False

        # Cleanup
        _token_cache.invalidate()

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_is_token_expired_token_expired(self, mock_get_conn):
        """Token avec expiration passée doit retourner True"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook, _token_cache

        _token_cache.invalidate()
        _token_cache.set_token('expired_token', 3600)  # validité réelle = 3240s (×0.9)

        hook = AMUEAPIHook()

        # Simule un now() 2 heures dans le futur → token expiré
        future = datetime.now() + timedelta(hours=2)
        with patch('amue.hooks.amue_api_hook.datetime') as mock_dt:
            mock_dt.now.return_value = future
            # `datetime.fromisoformat` doit conserver son comportement réel
            mock_dt.fromisoformat = datetime.fromisoformat
            assert hook._is_token_expired() is True

        _token_cache.invalidate()

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    @patch('amue.hooks.amue_api_hook.requests.post')
    def test_get_oauth_token_sets_expiration(self, mock_post, mock_get_conn):
        """get_oauth_token doit définir token_expires_at"""
        mock_conn = MagicMock()
        mock_conn.login = 'client_id'
        mock_conn.password = 'client_secret'
        mock_conn.extra = '{"token_url": "https://auth.example.com/token"}'
        mock_get_conn.return_value = mock_conn

        mock_response = MagicMock()
        mock_response.json.return_value = {
            'access_token': 'new_token',
            'expires_in': 3600,
            'token_type': 'Bearer'
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        from amue.hooks.amue_api_hook import AMUEAPIHook, _token_cache

        # Reset le cache avant le test
        _token_cache.invalidate()

        hook = AMUEAPIHook()
        token = hook.get_oauth_token()

        assert token == 'new_token'
        assert hook.access_token == 'new_token'
        # Vérifie que le token est dans le cache et valide
        cached_token, is_valid = _token_cache.get_token()
        assert cached_token == 'new_token'
        assert is_valid is True

        # Cleanup
        _token_cache.invalidate()

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_parse_connection_extra_valid_json(self, mock_get_conn):
        """_parse_connection_extra avec JSON valide"""
        mock_conn = MagicMock()
        mock_conn.extra = '{"token_url": "https://auth.example.com/token", "api_base_url": "https://api.example.com"}'
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra['token_url'] == 'https://auth.example.com/token'
        assert extra['api_base_url'] == 'https://api.example.com'

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_parse_connection_extra_invalid_json(self, mock_get_conn):
        """_parse_connection_extra avec JSON invalide retourne dict vide"""
        mock_conn = MagicMock()
        mock_conn.extra = 'invalid json'
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra == {}

    @patch('amue.hooks.amue_api_hook.get_airflow_connection')
    def test_parse_connection_extra_empty(self, mock_get_conn):
        """_parse_connection_extra avec extra vide retourne dict vide"""
        mock_conn = MagicMock()
        mock_conn.extra = None
        mock_get_conn.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra == {}
