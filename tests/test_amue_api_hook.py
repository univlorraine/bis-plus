"""
Tests unitaires pour AMUEAPIHook
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestAMUEAPIHookTokenExpiration:
    """Tests pour la gestion d'expiration du token OAuth"""

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_is_token_expired_no_token(self, mock_connection):
        """Sans token, _is_token_expired doit retourner True"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        assert hook._is_token_expired() is True

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_is_token_expired_token_still_valid(self, mock_connection):
        """Token avec expiration future doit retourner False"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook, _token_cache

        # Reset le cache et ajoute un token valide
        _token_cache.invalidate()
        _token_cache.set_token('valid_token', 3600)  # 1 heure

        hook = AMUEAPIHook()

        assert hook._is_token_expired() is False

        # Cleanup
        _token_cache.invalidate()

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_is_token_expired_token_expired(self, mock_connection):
        """Token avec expiration passée doit retourner True"""
        mock_conn = MagicMock()
        mock_conn.extra = '{}'
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        hook.access_token = 'expired_token'
        hook.token_expires_at = datetime.now() - timedelta(hours=1)

        assert hook._is_token_expired() is True

    @patch('amue.hooks.amue_api_hook.Connection')
    @patch('amue.hooks.amue_api_hook.requests.post')
    def test_get_oauth_token_sets_expiration(self, mock_post, mock_connection):
        """get_oauth_token doit définir token_expires_at"""
        mock_conn = MagicMock()
        mock_conn.login = 'client_id'
        mock_conn.password = 'client_secret'
        mock_conn.extra = '{"token_url": "https://auth.example.com/token"}'
        mock_connection.get.return_value = mock_conn

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

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_parse_connection_extra_valid_json(self, mock_connection):
        """_parse_connection_extra avec JSON valide"""
        mock_conn = MagicMock()
        mock_conn.extra = '{"token_url": "https://auth.example.com/token", "api_base_url": "https://api.example.com"}'
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra['token_url'] == 'https://auth.example.com/token'
        assert extra['api_base_url'] == 'https://api.example.com'

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_parse_connection_extra_invalid_json(self, mock_connection):
        """_parse_connection_extra avec JSON invalide retourne dict vide"""
        mock_conn = MagicMock()
        mock_conn.extra = 'invalid json'
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra == {}

    @patch('amue.hooks.amue_api_hook.Connection')
    def test_parse_connection_extra_empty(self, mock_connection):
        """_parse_connection_extra avec extra vide retourne dict vide"""
        mock_conn = MagicMock()
        mock_conn.extra = None
        mock_connection.get.return_value = mock_conn

        from amue.hooks.amue_api_hook import AMUEAPIHook

        hook = AMUEAPIHook()
        extra = hook._parse_connection_extra()

        assert extra == {}
