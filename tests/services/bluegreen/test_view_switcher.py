"""
Tests unitaires pour ViewSwitcher
"""
import pytest
from unittest.mock import MagicMock, patch, call


class TestViewSwitcherInit:
    """Tests pour l'initialisation de ViewSwitcher"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_init_default_hook(self, mock_create_hook):
        """Utilise le hook par défaut si non fourni"""
        mock_postgres_hook = MagicMock()
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()

        assert switcher.postgres_hook == mock_postgres_hook
        mock_create_hook.assert_called_once_with(schema='public')

    def test_init_custom_hook(self):
        """Utilise le hook personnalisé si fourni"""
        mock_postgres_hook = MagicMock()

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(postgres_hook=mock_postgres_hook)

        assert switcher.postgres_hook == mock_postgres_hook


class TestViewSwitcherGetTables:
    """Tests pour la récupération des tables"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_tables_in_schema(self, mock_create_hook):
        """Liste les tables dans un schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks',),
            ('prps',),
            ('fmbl',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        tables = switcher.get_tables_in_schema('splus_blue')

        assert tables == ['csks', 'prps', 'fmbl']
        mock_postgres_hook.get_records.assert_called_once()

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_tables_empty(self, mock_create_hook):
        """Retourne liste vide si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        tables = switcher.get_tables_in_schema('splus_blue')

        assert tables == []

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_views_in_schema(self, mock_create_hook):
        """Liste les vues dans un schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks',),
            ('prps',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        views = switcher.get_views_in_schema('splus')

        assert views == ['csks', 'prps']


class TestViewSwitcherSwitch:
    """Tests pour le switch des vues"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_success(self, mock_create_hook):
        """Switch réussi vers un schéma - 2 appels par table (DROP + CREATE)"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        # 1er appel: tables; 2e et 3e: colonnes de csks et prps (sans _source/_imported_at)
        mock_postgres_hook.get_records.side_effect = [
            [('csks',), ('prps',)],
            [('bukrs',), ('kostl',), ('datbi',)],
            [('kokrs',), ('belnr',)],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is True
        mock_conn.commit.assert_called_once()
        # 2 tables x 2 appels (DROP + CREATE) = 4
        assert mock_cursor.execute.call_count == 4
        mock_cursor.close.assert_called_once()

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_no_tables(self, mock_create_hook):
        """Retourne False si pas de tables"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is False

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_switch_views_to_schema_error_rollback(self, mock_create_hook):
        """Rollback en cas d'erreur"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL Error")
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.side_effect = [
            [('csks',)],
            [('bukrs',), ('kostl',)],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.switch_views_to_schema('splus_green')

        assert result is False
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestViewSwitcherDropCreate:
    """Tests pour le pattern DROP+CREATE"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_switch_uses_drop_create_not_replace(self, mock_create_hook):
        """Vérifie que le switch utilise DROP+CREATE (2 appels par table)"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.side_effect = [
            [('csks',)],
            [('bukrs',), ('kostl',)],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        switcher.switch_views_to_schema('splus_green')

        # 2 appels pour 1 table : DROP puis CREATE
        assert mock_cursor.execute.call_count == 2

        # Vérifie les objets SQL composés passés à execute
        from psycopg2 import sql as psql
        calls = mock_cursor.execute.call_args_list
        drop_composed = calls[0][0][0]
        create_composed = calls[1][0][0]

        # Les deux sont des objets SQL Composed
        assert isinstance(drop_composed, psql.Composed)
        assert isinstance(create_composed, psql.Composed)

        # Vérifie les templates SQL via les strings internes (items sql.SQL uniquement)
        drop_strings = [s._wrapped if isinstance(s, psql.SQL) else s for s in drop_composed.seq]
        create_strings = [s._wrapped if isinstance(s, psql.SQL) else s for s in create_composed.seq]

        drop_text = ''.join(s for s in drop_strings if isinstance(s, str))
        create_text = ''.join(s for s in create_strings if isinstance(s, str))

        assert 'DROP VIEW IF EXISTS' in drop_text
        assert 'CREATE VIEW' in create_text
        assert 'REPLACE' not in create_text


class TestViewSwitcherGetViewColumns:
    """Tests pour _get_view_columns"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_view_columns_excludes_meta(self, mock_create_hook):
        """Exclut _source et _imported_at"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('bukrs',), ('kostl',), ('datbi',)
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        cols = switcher._get_view_columns('csks', 'splus_blue')

        assert cols == ['bukrs', 'kostl', 'datbi']
        # Vérifie que la requête exclut bien les colonnes techniques
        call_args = mock_postgres_hook.get_records.call_args
        query = call_args[0][0]
        assert '_source' in query
        assert '_imported_at' in query

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_view_columns_empty(self, mock_create_hook):
        """Retourne liste vide si pas de colonnes"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        cols = switcher._get_view_columns('csks', 'splus_blue')

        assert cols == []


class TestViewSwitcherVerify:
    """Tests pour la vérification des vues"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_verify_views_point_to_correct(self, mock_create_hook):
        """Vérifie que les vues pointent vers le bon schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks', 'SELECT * FROM splus_blue.csks'),
            ('prps', 'SELECT * FROM splus_blue.prps')
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is True

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_verify_views_point_to_wrong(self, mock_create_hook):
        """Détecte les vues pointant vers le mauvais schéma"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks', 'SELECT * FROM splus_green.csks'),  # Mauvais schéma
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is False

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_verify_views_empty(self, mock_create_hook):
        """Retourne True si pas de vues"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = []
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.verify_views_point_to('splus_blue')

        assert result is True


class TestViewSwitcherCreateView:
    """Tests pour la création de vues individuelles"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_create_view_for_table_success(self, mock_create_hook):
        """Crée une vue avec succès (DROP + CREATE) sans _source ni _imported_at"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('bukrs',), ('kostl',), ('datbi',)]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.create_view_for_table('csks', 'splus_blue')

        assert result is True
        # 2 appels : DROP + CREATE
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_create_view_for_table_no_commit(self, mock_create_hook):
        """Crée une vue sans commit (DROP + CREATE)"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = [('bukrs',), ('kostl',)]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.create_view_for_table('csks', 'splus_blue', commit=False)

        assert result is True
        # 2 appels : DROP + CREATE
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_not_called()

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_create_view_fallback_select_star_when_no_columns(self, mock_create_hook):
        """Fallback SELECT * si aucune colonne trouvée"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.return_value = []  # Aucune colonne
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.create_view_for_table('csks', 'splus_blue')

        assert result is True
        assert mock_cursor.execute.call_count == 2


class TestViewSwitcherCurrentTarget:
    """Tests pour la détection du schéma cible actuel"""

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_blue(self, mock_create_hook):
        """Détecte splus_blue comme schéma cible"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = ('SELECT * FROM splus_blue.csks',)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result == 'splus_blue'

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_green(self, mock_create_hook):
        """Détecte splus_green comme schéma cible"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = ('SELECT * FROM splus_green.csks',)
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result == 'splus_green'

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_get_current_target_schema_none(self, mock_create_hook):
        """Retourne None si pas de vues"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_first.return_value = None
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher.get_current_target_schema()

        assert result is None
