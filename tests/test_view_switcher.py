"""
Tests unitaires pour ViewSwitcher
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch, call

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestViewSwitcherInit:
    """Tests pour l'initialisation de ViewSwitcher"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_init_default_hook(self, mock_create_hook):
        """Utilise le hook par défaut si non fourni"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()

        assert switcher.postgres_hook == mock_postgres_hook
        mock_create_hook.assert_called_once_with(schema='public')

    def test_init_custom_hook(self):
        """Utilise le hook personnalisé si fourni"""
        mock_postgres_hook = MagicMock()

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(postgres_hook=mock_postgres_hook)

        assert switcher.postgres_hook == mock_postgres_hook


class TestViewSwitcherGetTables:
    """Tests pour la récupération des tables"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_tables_in_schema(self, mock_create_hook):
        """Liste les tables dans un schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks',),
            ('prps',),
            ('fmbl',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        tables = switcher.get_tables_in_schema('splus_blue')

        assert tables == ['csks', 'prps', 'fmbl']
        mock_postgres_hook.get_records.assert_called_once()

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_tables_empty(self, mock_create_hook):
        """Retourne liste vide si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        tables = switcher.get_tables_in_schema('splus_blue')

        assert tables == []

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_views_in_schema(self, mock_create_hook):
        """Liste les vues dans un schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks',),
            ('prps',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        views = switcher.get_views_in_schema('splus')

        assert views == ['csks', 'prps']


class TestViewSwitcherSwitch:
    """Tests pour le switch des vues"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_success(self, mock_create_hook):
        """Switch réussi vers un schéma"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',), ('prps',)]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is True
        mock_conn.commit.assert_called_once()
        assert mock_cursor.execute.call_count == 2
        mock_cursor.close.assert_called_once()

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_no_tables(self, mock_create_hook):
        """Retourne False si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is False

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_error_rollback(self, mock_create_hook):
        """Rollback en cas d'erreur"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL Error")
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('csks',)]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is False
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestViewSwitcherVerify:
    """Tests pour la vérification des vues"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_verify_views_point_to_correct(self, mock_create_hook):
        """Vérifie que les vues pointent vers le bon schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks', 'SELECT * FROM splus_blue.csks'),
            ('prps', 'SELECT * FROM splus_blue.prps')
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is True

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_verify_views_point_to_wrong(self, mock_create_hook):
        """Détecte les vues pointant vers le mauvais schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks', 'SELECT * FROM splus_green.csks'),  # Mauvais schéma
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is False

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_verify_views_empty(self, mock_create_hook):
        """Retourne True si pas de vues"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is True


class TestViewSwitcherCreateView:
    """Tests pour la création de vues individuelles"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_create_view_for_table_success(self, mock_create_hook):
        """Crée une vue avec succès"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.create_view_for_table('csks', 'splus_blue')

        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_create_view_for_table_no_commit(self, mock_create_hook):
        """Crée une vue sans commit"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.create_view_for_table('csks', 'splus_blue', commit=False)

        assert result is True
        mock_conn.commit.assert_not_called()


class TestViewSwitcherCurrentTarget:
    """Tests pour la détection du schéma cible actuel"""

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_blue(self, mock_create_hook):
        """Détecte splus_blue comme schéma cible"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = ('SELECT * FROM splus_blue.csks',)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result == 'splus_blue'

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_green(self, mock_create_hook):
        """Détecte splus_green comme schéma cible"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = ('SELECT * FROM splus_green.csks',)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result == 'splus_green'

    @patch('amue.services.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_none(self, mock_create_hook):
        """Retourne None si pas de vues"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = None
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result is None
