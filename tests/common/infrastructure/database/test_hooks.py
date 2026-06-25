# tests/utils/database/test_hooks.py
"""
Tests unitaires pour le module hooks.
"""
import pytest
from unittest.mock import patch, MagicMock

from common.infrastructure.database.hooks import (
    create_postgres_hook,
    create_bluegreen_hook,
    HookManager,
    POSTGRES_DEFAULT_CONN_ID,
    POSTGRES_DEFAULT_SCHEMA,
    SCHEMA_BLUE,
    SCHEMA_GREEN,
)


class TestCreatePostgresHook:
    """Tests pour create_postgres_hook."""

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_creates_hook_with_defaults(self, mock_hook_class):
        """Test création avec paramètres par défaut."""
        create_postgres_hook()

        mock_hook_class.assert_called_once_with(
            postgres_conn_id=POSTGRES_DEFAULT_CONN_ID,
            options=f'-c search_path={POSTGRES_DEFAULT_SCHEMA}'
        )

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_creates_hook_with_custom_conn_id(self, mock_hook_class):
        """Test création avec conn_id personnalisé."""
        create_postgres_hook(conn_id='custom_db')

        mock_hook_class.assert_called_once()
        call_kwargs = mock_hook_class.call_args[1]
        assert call_kwargs['postgres_conn_id'] == 'custom_db'

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_creates_hook_with_custom_schema(self, mock_hook_class):
        """Test création avec schéma personnalisé."""
        create_postgres_hook(schema='public')

        mock_hook_class.assert_called_once()
        call_kwargs = mock_hook_class.call_args[1]
        assert 'public' in call_kwargs['options']

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_bluegreen_schema_overrides_schema(self, mock_hook_class):
        """Test que bluegreen_schema a priorité sur schema."""
        create_postgres_hook(schema='public', bluegreen_schema='splus_blue')

        mock_hook_class.assert_called_once()
        call_kwargs = mock_hook_class.call_args[1]
        assert 'splus_blue' in call_kwargs['options']
        assert 'public' not in call_kwargs['options']


class TestCreateBluegreenHook:
    """Tests pour create_bluegreen_hook."""

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_creates_hook_for_blue_schema(self, mock_hook_class):
        """Test création pour schéma blue."""
        create_bluegreen_hook(SCHEMA_BLUE)

        mock_hook_class.assert_called_once()
        call_kwargs = mock_hook_class.call_args[1]
        assert SCHEMA_BLUE in call_kwargs['options']

    @patch('common.infrastructure.database.hooks.PostgresHook')
    def test_creates_hook_for_green_schema(self, mock_hook_class):
        """Test création pour schéma green."""
        create_bluegreen_hook(SCHEMA_GREEN)

        mock_hook_class.assert_called_once()
        call_kwargs = mock_hook_class.call_args[1]
        assert SCHEMA_GREEN in call_kwargs['options']

    def test_raises_on_invalid_schema(self):
        """Test lève erreur sur schéma invalide."""
        with pytest.raises(ValueError) as exc_info:
            create_bluegreen_hook('invalid_schema')

        assert "Schéma invalide" in str(exc_info.value)
        assert SCHEMA_BLUE in str(exc_info.value)
        assert SCHEMA_GREEN in str(exc_info.value)

    def test_raises_on_default_schema(self):
        """Test lève erreur sur schéma par défaut."""
        with pytest.raises(ValueError):
            create_bluegreen_hook('splus')


class TestHookManagerSingleton:
    """Tests pour le pattern singleton de HookManager."""

    def teardown_method(self):
        """Reset singleton après chaque test."""
        HookManager._instance = None
        HookManager._local = __import__('threading').local()

    def test_singleton_returns_same_instance(self):
        """Test que le singleton retourne la même instance."""
        manager1 = HookManager()
        manager2 = HookManager()

        assert manager1 is manager2

    def test_reset_clears_hooks(self):
        """Test que reset efface le hook (thread-local)."""
        manager = HookManager()
        manager._local.postgres_hook = MagicMock()

        manager.reset()

        assert manager._local.postgres_hook is None


class TestHookManagerPostgresHook:
    """Tests pour HookManager.postgres_hook."""

    def teardown_method(self):
        """Reset singleton après chaque test."""
        HookManager._instance = None
        HookManager._local = __import__('threading').local()

    @patch('common.infrastructure.database.hooks.create_postgres_hook')
    def test_lazy_loads_postgres_hook(self, mock_create):
        """Test lazy loading du hook PostgreSQL."""
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook

        manager = HookManager()
        hook = manager.postgres_hook

        mock_create.assert_called_once()
        assert hook is mock_hook

    @patch('common.infrastructure.database.hooks.create_postgres_hook')
    def test_reuses_postgres_hook(self, mock_create):
        """Test réutilisation du hook PostgreSQL."""
        mock_hook = MagicMock()
        mock_create.return_value = mock_hook

        manager = HookManager()
        hook1 = manager.postgres_hook
        hook2 = manager.postgres_hook

        mock_create.assert_called_once()
        assert hook1 is hook2
