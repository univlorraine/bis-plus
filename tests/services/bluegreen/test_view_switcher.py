"""
Tests unitaires pour ViewSwitcher
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Répertoire inexistant pour isoler les tests des vues custom réelles
_NO_CUSTOM_VIEWS = Path("/nonexistent/custom_views")


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
        # 1er appel: tables; 2e: toutes les colonnes en batch (table_name, column_name)
        mock_postgres_hook.get_records.side_effect = [
            [('csks',), ('prps',)],
            [('csks', 'bukrs'), ('csks', 'kostl'), ('csks', 'datbi'),
             ('prps', 'kokrs'), ('prps', 'belnr')],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=_NO_CUSTOM_VIEWS)
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
            [('csks', 'bukrs'), ('csks', 'kostl')],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=_NO_CUSTOM_VIEWS)
        result = switcher.switch_views_to_schema('splus_green')

        assert result is False
        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_switch_views_invalid_schema(self):
        """Lève ValueError si le schéma cible n'est pas splus_blue ou splus_green"""
        mock_postgres_hook = MagicMock()

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(postgres_hook=mock_postgres_hook)

        with pytest.raises(ValueError, match="Schéma invalide"):
            switcher.switch_views_to_schema('splus_evil')

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_switch_views_single_columns_query(self, mock_create_hook):
        """Une seule requête batch pour toutes les colonnes (pas N+1)"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.side_effect = [
            [('csks',), ('prps',), ('fmbl',)],       # 1er appel : tables
            [                                          # 2e appel : toutes les colonnes
                ('csks', 'bukrs'), ('csks', 'kostl'),
                ('prps', 'posid'),
                ('fmbl', 'bukrs'), ('fmbl', 'gjahr'),
            ],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=_NO_CUSTOM_VIEWS)
        result = switcher.switch_views_to_schema('splus_blue')

        assert result is True
        # Exactement 2 appels get_records : 1 tables + 1 batch colonnes
        assert mock_postgres_hook.get_records.call_count == 2


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
            [('csks', 'bukrs'), ('csks', 'kostl')],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=_NO_CUSTOM_VIEWS)
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
    def test_get_all_view_columns_returns_dict(self, mock_create_hook):
        """_get_all_view_columns retourne un dict keyed par table_name"""
        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_records.return_value = [
            ('csks', 'bukrs'),
            ('csks', 'kostl'),
            ('prps', 'posid'),
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher()
        result = switcher._get_all_view_columns(['csks', 'prps'], 'splus_blue')

        assert result == {'csks': ['bukrs', 'kostl'], 'prps': ['posid']}
        # Vérifie que la requête exclut les colonnes techniques
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


class TestViewSwitcherCustomViews:
    """Tests pour le chargement et l'application des vues personnalisées"""

    def test_no_custom_views_dir_is_noop(self):
        """Si le dossier n'existe pas, _load_custom_view_sqls retourne liste vide"""
        mock_postgres_hook = MagicMock()

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(
            postgres_hook=mock_postgres_hook,
            custom_views_dir=Path("/nonexistent/path/custom_views")
        )
        result = switcher._load_custom_view_sqls("splus_green")

        assert result == []

    def test_schema_placeholder_substituted(self, tmp_path):
        """Le placeholder {target_schema} est remplacé par le schéma cible"""
        sql_file = tmp_path / "01_test.sql"
        sql_file.write_text(
            "DROP VIEW IF EXISTS splus.v_test;\n"
            "CREATE VIEW splus.v_test AS SELECT id FROM {target_schema}.csks;",
            encoding="utf-8"
        )

        mock_postgres_hook = MagicMock()

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(postgres_hook=mock_postgres_hook, custom_views_dir=tmp_path)
        sqls = switcher._load_custom_view_sqls("splus_green")

        assert len(sqls) == 1
        assert "{target_schema}" not in sqls[0]
        assert "splus_green" in sqls[0]
        assert "splus_blue" not in sqls[0]

    def test_custom_views_sorted_alphabetically(self, tmp_path):
        """Les fichiers sont exécutés dans l'ordre alphabétique"""
        (tmp_path / "02_b.sql").write_text("SELECT 2 FROM {target_schema}.t;", encoding="utf-8")
        (tmp_path / "01_a.sql").write_text("SELECT 1 FROM {target_schema}.t;", encoding="utf-8")
        (tmp_path / "03_c.sql").write_text("SELECT 3 FROM {target_schema}.t;", encoding="utf-8")

        mock_postgres_hook = MagicMock()

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(postgres_hook=mock_postgres_hook, custom_views_dir=tmp_path)
        sqls = switcher._load_custom_view_sqls("splus_blue")

        assert len(sqls) == 3
        assert "SELECT 1" in sqls[0]
        assert "SELECT 2" in sqls[1]
        assert "SELECT 3" in sqls[2]

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_custom_views_applied_in_same_transaction(self, mock_create_hook, tmp_path):
        """Les vues custom sont exécutées dans le même cursor que les vues standard"""
        sql_file = tmp_path / "01_custom.sql"
        sql_file.write_text(
            "DROP VIEW IF EXISTS splus.custom_v;\n"
            "CREATE VIEW splus.custom_v AS SELECT id FROM {target_schema}.csks;",
            encoding="utf-8"
        )

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        # 1er appel: tables; 2e: colonnes en batch
        mock_postgres_hook.get_records.side_effect = [
            [('csks',)],
            [('csks', 'bukrs'), ('csks', 'kostl')],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=tmp_path)
        result = switcher.switch_views_to_schema('splus_green')

        assert result is True
        # 2 appels pour csks (DROP + CREATE) + 1 appel pour la vue custom = 3
        assert mock_cursor.execute.call_count == 3
        # Un seul commit (transaction atomique)
        mock_conn.commit.assert_called_once()

    @patch('amue.services.bluegreen.view_switcher.create_postgres_hook')
    def test_custom_views_dir_empty_no_extra_calls(self, mock_create_hook, tmp_path):
        """Un dossier custom_views vide ne génère pas d'appels cursor supplémentaires"""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_postgres_hook = MagicMock()
        mock_postgres_hook.get_conn.return_value = mock_conn
        mock_postgres_hook.get_records.side_effect = [
            [('csks',)],
            [('csks', 'bukrs')],
        ]
        mock_create_hook.return_value = mock_postgres_hook

        from amue.services.bluegreen.view_switcher import ViewSwitcher

        switcher = ViewSwitcher(custom_views_dir=tmp_path)  # dossier vide
        result = switcher.switch_views_to_schema('splus_green')

        assert result is True
        # Exactement 2 appels (DROP + CREATE pour csks), rien de plus
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
