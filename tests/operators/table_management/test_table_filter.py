"""
Tests unitaires pour AMUETableFilter
"""
import pytest
import json
from unittest.mock import MagicMock, patch


class TestTableFilterInit:
    """Tests pour l'initialisation de AMUETableFilter"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_init_with_config(self, mock_varmgr):
        """Initialisation avec configuration fournie"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'primary_key': 'id'},
            {'name': 'PRKS', 'primary_key': 'code'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        assert len(filter_obj.tables_config) == 2
        assert filter_obj.tables_config[0]['name'] == 'CSKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_init_load_config(self, mock_varmgr):
        """Initialisation charge la config depuis les variables"""
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'primary_key': 'id'}
        ])

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert len(filter_obj.tables_config) == 1


class TestTableFilterCheckTablesExist:
    """Tests pour _check_tables_exist_in_status"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_all_tables_exist(self, mock_varmgr):
        """Toutes les tables existent dans le statut"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS'},
            {'name': 'PRKS'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'},
            'PRKS': {'status': 'OK'}
        }

        missing = filter_obj._check_tables_exist_in_status(current_status)

        assert len(missing) == 0

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_some_tables_missing(self, mock_varmgr):
        """Certaines tables sont manquantes"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS'},
            {'name': 'PRKS'},
            {'name': 'UNKNOWN'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'},
            'PRKS': {'status': 'OK'}
        }

        missing = filter_obj._check_tables_exist_in_status(current_status)

        assert 'UNKNOWN' in missing

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_table_status_not_ok(self, mock_varmgr):
        """Table avec statut != OK est considérée manquante"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS'},
            {'name': 'PRKS'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'},
            'PRKS': {'status': 'KO'}  # Statut non OK
        }

        missing = filter_obj._check_tables_exist_in_status(current_status)

        assert 'PRKS' in missing


class TestTableFilterFilterTables:
    """Tests pour filter_tables"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_filter_tables_success(self, mock_varmgr):
        """Filtrage réussi avec toutes les tables OK"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'primary_key': 'id', 'delta': '', 'last_import': ''},
            {'name': 'PRKS', 'primary_key': 'code', 'delta': 'date_modif', 'last_import': '2024-01-01'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK', 'mode': 'FULL'},
            'PRKS': {'status': 'OK', 'mode': 'DELTA'}
        }

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 2
        # CSKS sans delta = full import
        csks = next(t for t in result if t['name'] == 'CSKS')
        assert csks['import_type'] == 'full'
        # PRKS avec delta et last_import = differential
        prks = next(t for t in result if t['name'] == 'PRKS')
        assert prks['import_type'] == 'differential'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_filter_tables_missing_raises_error(self, mock_varmgr):
        """Tables manquantes lèvent une erreur"""
        from amue.operators.table_management.table_filter import AMUETableFilter, TableNotFoundError

        tables_config = [
            {'name': 'CSKS'},
            {'name': 'UNKNOWN'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'}
        }

        with pytest.raises(TableNotFoundError) as exc_info:
            filter_obj.filter_tables(current_status)

        assert 'UNKNOWN' in exc_info.value.missing_tables


class TestTableFilterEnrichConfig:
    """Tests pour _enrich_table_config"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_enrich_table_config_defaults(self, mock_varmgr):
        """Enrichissement ajoute les valeurs par défaut"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {'name': 'CSKS'}
        current_status = {'status': 'OK', 'mode': 'FULL'}

        enriched = filter_obj._enrich_table_config(table_config, current_status)

        assert enriched['primary_key'] == ''
        assert enriched['delta'] == ''
        assert enriched['last_import'] == ''
        assert enriched['fingerprint_API'] == ''
        assert enriched['fingerprint_UL'] == ''
        assert enriched['current_status'] == current_status

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_enrich_table_config_needs_pk_update(self, mock_varmgr):
        """Enrichissement détecte si PK doit être mis à jour"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        # Sans primary_key -> needs_pk_update = True
        table_config = {'name': 'CSKS'}
        enriched = filter_obj._enrich_table_config(table_config, {'status': 'OK'})
        assert enriched['needs_pk_update'] is True

        # Avec primary_key -> needs_pk_update = False
        table_config = {'name': 'CSKS', 'primary_key': 'id'}
        enriched = filter_obj._enrich_table_config(table_config, {'status': 'OK'})
        assert enriched['needs_pk_update'] is False

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_enrich_table_config_extracts_table_finish(self, mock_varmgr):
        """Enrichissement extrait table_finish depuis le statut API"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        # Avec finish dans le statut
        table_config = {'name': 'CSKS'}
        current_status = {'status': 'OK', 'mode': 'FULL', 'finish': '2024-01-15 03:45:00'}
        enriched = filter_obj._enrich_table_config(table_config, current_status)
        assert enriched['table_finish'] == '2024-01-15 03:45:00'

        # Sans finish dans le statut
        current_status_no_finish = {'status': 'OK', 'mode': 'FULL'}
        enriched2 = filter_obj._enrich_table_config(table_config, current_status_no_finish)
        assert enriched2['table_finish'] == ''


class TestTableFilterShouldProcess:
    """Tests pour _should_process_table"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_should_process_ok_status(self, mock_varmgr):
        """Table avec statut OK doit être traitée"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'}
        }

        result = filter_obj._should_process_table(table_config)

        assert result is True
        assert table_config['to_process'] is True

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_should_not_process_ko_status(self, mock_varmgr):
        """Table avec statut KO ne doit pas être traitée"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'name': 'CSKS',
            'current_status': {'status': 'KO'}
        }

        result = filter_obj._should_process_table(table_config)

        assert result is False

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_import_type_full(self, mock_varmgr):
        """Import type full si pas de last_import ou delta"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'last_import': '',
            'delta': ''
        }

        filter_obj._should_process_table(table_config)

        assert table_config['import_type'] == 'full'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_import_type_differential(self, mock_varmgr):
        """Import type differential si last_import et delta présents"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'name': 'CSKS',
            'current_status': {'status': 'OK'},
            'last_import': '2024-01-01',
            'delta': 'date_modif'
        }

        filter_obj._should_process_table(table_config)

        assert table_config['import_type'] == 'differential'


class TestTableNotFoundError:
    """Tests pour TableNotFoundError exception"""

    def test_exception_message(self):
        """Message d'exception formaté correctement"""
        from amue.operators.table_management.table_filter import TableNotFoundError

        exc = TableNotFoundError(
            missing_tables=['TABLE1', 'TABLE2'],
            configured_count=5,
            found_count=3
        )

        assert exc.missing_tables == ['TABLE1', 'TABLE2']
        assert exc.configured_count == 5
        assert exc.found_count == 3
        assert 'TABLE1' in str(exc)
        assert 'TABLE2' in str(exc)
        assert '2 table(s)' in str(exc)


class TestTableFilterLoadConfig:
    """Tests pour _load_config"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_load_config_json_string(self, mock_varmgr):
        """Charge une configuration JSON string"""
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'primary_key': 'id'}
        ])

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert len(filter_obj.tables_config) == 1
        assert filter_obj.tables_config[0]['name'] == 'CSKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_load_config_already_list(self, mock_varmgr):
        """Config déjà en liste"""
        mock_varmgr.get.return_value = [
            {'name': 'CSKS', 'primary_key': 'id'}
        ]

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert len(filter_obj.tables_config) == 1

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_load_config_not_list_returns_empty(self, mock_varmgr):
        """Config JSON valide mais pas une liste retourne liste vide"""
        # JSON valide mais c'est un dict, pas une liste
        mock_varmgr.get.return_value = json.dumps({"name": "CSKS"})

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert filter_obj.tables_config == []


class TestTableFilterEnableAttribute:
    """Tests pour l'attribut 'enable' des tables"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_split_by_enable_all_enabled(self, mock_varmgr):
        """Toutes les tables sont activées par défaut"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS'},  # enable non défini = True
            {'name': 'PRKS', 'enable': True}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 2
        assert len(disabled) == 0

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_split_by_enable_some_disabled(self, mock_varmgr):
        """Tables avec enable=false sont dans la liste disabled"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'enable': True},
            {'name': 'PRKS', 'enable': False},
            {'name': 'COST'}  # enable non défini = True
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 2
        assert len(disabled) == 1
        assert disabled[0]['name'] == 'PRKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_split_by_enable_string_values(self, mock_varmgr):
        """Gestion des valeurs string pour enable"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'enable': 'true'},
            {'name': 'PRKS', 'enable': 'false'},
            {'name': 'COST', 'enable': 'yes'},
            {'name': 'BKPF', 'enable': '1'},
            {'name': 'BSEG', 'enable': 'oui'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 4  # CSKS, COST, BKPF, BSEG
        assert len(disabled) == 1  # PRKS

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_filter_tables_ignores_disabled(self, mock_varmgr):
        """filter_tables ignore les tables désactivées"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'enable': True, 'primary_key': 'id', 'delta': '', 'last_import': ''},
            {'name': 'PRKS', 'enable': False, 'primary_key': 'code', 'delta': '', 'last_import': ''},
            {'name': 'COST', 'primary_key': 'id', 'delta': '', 'last_import': ''}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK', 'mode': 'FULL'},
            'COST': {'status': 'OK', 'mode': 'FULL'}
            # PRKS n'est pas dans le statut car désactivée
        }

        result = filter_obj.filter_tables(current_status)

        # Seules CSKS et COST sont retournées (PRKS est désactivée)
        assert len(result) == 2
        table_names = [t['name'] for t in result]
        assert 'CSKS' in table_names
        assert 'COST' in table_names
        assert 'PRKS' not in table_names

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_filter_tables_disabled_not_checked_in_status(self, mock_varmgr):
        """Tables désactivées ne sont pas vérifiées dans le statut API"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'enable': True, 'primary_key': 'id', 'delta': '', 'last_import': ''},
            {'name': 'UNKNOWN', 'enable': False}  # Désactivée, ne devrait pas causer d'erreur
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK', 'mode': 'FULL'}
            # UNKNOWN n'existe pas mais c'est OK car désactivée
        }

        # Ne devrait pas lever d'exception
        result = filter_obj.filter_tables(current_status)

        assert len(result) == 1
        assert result[0]['name'] == 'CSKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('amue.operators.table_management.table_filter.VarMgr')
    def test_filter_tables_all_disabled(self, mock_varmgr):
        """Toutes les tables désactivées retourne liste vide"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'name': 'CSKS', 'enable': False},
            {'name': 'PRKS', 'enable': False}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {}

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 0
