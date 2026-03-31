"""
Tests unitaires pour AMUETableFilter
"""
import pytest
from unittest.mock import MagicMock, patch


class TestTableFilterInit:
    """Tests pour l'initialisation de AMUETableFilter"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_init_with_config(self):
        """Initialisation avec configuration fournie (sans chargement BDD)"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'primary_key': 'id'},
            {'table_name': 'PRKS', 'primary_key': 'code'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        assert len(filter_obj.tables_config) == 2
        assert filter_obj.tables_config[0]['table_name'] == 'CSKS'
        assert filter_obj._last_report_start == ''

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('common.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_init_load_config(self, mock_tcm_cls, mock_admin_cls):
        """Initialisation charge la config et last_report_start depuis la BDD"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'table_name': 'CSKS', 'primary_key': 'id', 'enable': True,
             'delta': '', 'fingerprint_API': '', 'fingerprint_UL': ''}
        ]

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_report_start.return_value = '2026-02-17T10:08:19+00:00'

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert len(filter_obj.tables_config) == 1
        assert filter_obj._last_report_start == '2026-02-17T10:08:19+00:00'


class TestTableFilterCheckTablesExist:
    """Tests pour _check_tables_exist_in_status"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_all_tables_exist(self):
        """Toutes les tables existent dans le statut"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS'},
            {'table_name': 'PRKS'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'},
            'PRKS': {'status': 'OK'}
        }

        missing = filter_obj._check_tables_exist_in_status(current_status)

        assert len(missing) == 0

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_some_tables_missing(self):
        """Certaines tables sont manquantes"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS'},
            {'table_name': 'PRKS'},
            {'table_name': 'UNKNOWN'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK'},
            'PRKS': {'status': 'OK'}
        }

        missing = filter_obj._check_tables_exist_in_status(current_status)

        assert 'UNKNOWN' in missing

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_table_status_not_ok(self):
        """Table avec statut != OK est considérée manquante"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS'},
            {'table_name': 'PRKS'}
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
    def test_filter_tables_full_import(self):
        """Tables sans delta = full import même avec last_report_start"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'primary_key': 'id', 'delta': ''},
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        filter_obj._last_report_start = '2026-02-17T10:08:19+00:00'

        current_status = {
            'CSKS': {'status': 'OK', 'mode': 'FULL'},
        }

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 1
        csks = result[0]
        assert csks['import_type'] == 'full'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_differential_with_report_start(self):
        """Table avec delta + last_report_start = delta"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'PRKS', 'primary_key': 'code', 'delta': 'date_modif'},
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        filter_obj._last_report_start = '2026-02-17T10:08:19+00:00'

        current_status = {
            'PRKS': {'status': 'OK', 'mode': 'DELTA'},
        }

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 1
        prks = result[0]
        assert prks['import_type'] == 'delta'
        assert prks['last_import'] == '2026-02-17T10:08:19+00:00'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_no_report_start_forces_full(self):
        """Sans last_report_start, tables delta importées en full"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'PRKS', 'primary_key': 'code', 'delta': 'date_modif'},
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        filter_obj._last_report_start = ''  # Pas de timestamp global

        current_status = {
            'PRKS': {'status': 'OK', 'mode': 'FULL'},
        }

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 1
        assert result[0]['import_type'] == 'full'
        assert 'last_import' not in result[0]

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_missing_raises_error(self):
        """Tables manquantes lèvent une erreur"""
        from amue.operators.table_management.table_filter import AMUETableFilter, TableNotFoundError

        tables_config = [
            {'table_name': 'CSKS'},
            {'table_name': 'UNKNOWN'}
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
    def test_enrich_table_config_defaults(self):
        """Enrichissement ajoute les valeurs par défaut (sans last_import)"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {'table_name': 'CSKS'}
        current_status = {'status': 'OK', 'mode': 'FULL'}

        enriched = filter_obj._enrich_table_config(table_config, current_status)

        assert enriched['primary_key'] == ''
        assert enriched['delta'] == ''
        assert enriched['fingerprint_API'] == ''
        assert enriched['fingerprint_UL'] == ''
        assert enriched['current_status'] == current_status
        # last_import n'est pas injecté à l'enrichissement — seulement dans _should_process_table
        assert 'last_import' not in enriched

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_enrich_table_config_needs_pk_update(self):
        """Enrichissement détecte si PK doit être mis à jour"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        # Sans primary_key -> needs_pk_update = True
        table_config = {'table_name': 'CSKS'}
        enriched = filter_obj._enrich_table_config(table_config, {'status': 'OK'})
        assert enriched['needs_pk_update'] is True

        # Avec primary_key -> needs_pk_update = False
        table_config = {'table_name': 'CSKS', 'primary_key': 'id'}
        enriched = filter_obj._enrich_table_config(table_config, {'status': 'OK'})
        assert enriched['needs_pk_update'] is False

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_enrich_table_config_extracts_table_finish(self):
        """Enrichissement extrait table_finish depuis le statut API"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        # Avec finish dans le statut
        table_config = {'table_name': 'CSKS'}
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
    def test_should_process_ok_status(self):
        """Table avec statut OK doit être traitée"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'}
        }

        result = filter_obj._should_process_table(table_config)

        assert result is True
        assert table_config['to_process'] is True

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_should_not_process_ko_status(self):
        """Table avec statut KO ne doit pas être traitée"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])

        table_config = {
            'table_name': 'CSKS',
            'current_status': {'status': 'KO'}
        }

        result = filter_obj._should_process_table(table_config)

        assert result is False

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_import_type_full_no_delta(self):
        """Import type full si pas de colonne delta"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])
        filter_obj._last_report_start = '2026-02-17T10:08:19+00:00'

        table_config = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'delta': ''
        }

        filter_obj._should_process_table(table_config)

        assert table_config['import_type'] == 'full'
        assert 'last_import' not in table_config

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_import_type_full_no_report_start(self):
        """Import type full si pas de last_report_start même avec delta"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])
        filter_obj._last_report_start = ''

        table_config = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'delta': 'date_modif'
        }

        filter_obj._should_process_table(table_config)

        assert table_config['import_type'] == 'full'
        assert 'last_import' not in table_config

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_import_type_differential(self):
        """Import type delta si last_report_start et delta présents"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter(tables_config=[])
        filter_obj._last_report_start = '2026-02-17T10:08:19+00:00'

        table_config = {
            'table_name': 'CSKS',
            'current_status': {'status': 'OK'},
            'delta': 'date_modif'
        }

        filter_obj._should_process_table(table_config)

        assert table_config['import_type'] == 'delta'
        assert table_config['last_import'] == '2026-02-17T10:08:19+00:00'


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
    @patch('common.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_load_config_from_db(self, mock_tcm_cls, mock_admin_cls):
        """Charge une configuration depuis la BDD"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'table_name': 'CSKS', 'primary_key': 'id', 'enable': True,
             'delta': '', 'fingerprint_API': '', 'fingerprint_UL': ''}
        ]

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_report_start.return_value = None

        from amue.operators.table_management.table_filter import AMUETableFilter

        filter_obj = AMUETableFilter()

        assert len(filter_obj.tables_config) == 1
        assert filter_obj.tables_config[0]['table_name'] == 'CSKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    @patch('common.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_load_config_empty(self, mock_tcm_cls, mock_admin_cls):
        """Config vide lève ValueError (aucune table configurée)"""
        import pytest
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = []

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_report_start.return_value = None

        from amue.operators.table_management.table_filter import AMUETableFilter

        with pytest.raises(ValueError, match="Aucune table configurée"):
            AMUETableFilter()


class TestTableFilterEnableAttribute:
    """Tests pour l'attribut 'enable' des tables"""

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_split_by_enable_all_enabled(self):
        """Toutes les tables sont activées par défaut"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS'},  # enable non défini = True
            {'table_name': 'PRKS', 'enable': True}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 2
        assert len(disabled) == 0

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_split_by_enable_some_disabled(self):
        """Tables avec enable=false sont dans la liste disabled"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'enable': True},
            {'table_name': 'PRKS', 'enable': False},
            {'table_name': 'COST'}  # enable non défini = True
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 2
        assert len(disabled) == 1
        assert disabled[0]['table_name'] == 'PRKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_split_by_enable_string_values(self):
        """Gestion des valeurs string pour enable"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'enable': 'true'},
            {'table_name': 'PRKS', 'enable': 'false'},
            {'table_name': 'COST', 'enable': 'yes'},
            {'table_name': 'BKPF', 'enable': '1'},
            {'table_name': 'BSEG', 'enable': 'oui'}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)
        enabled, disabled = filter_obj._split_by_enable_status()

        assert len(enabled) == 4  # CSKS, COST, BKPF, BSEG
        assert len(disabled) == 1  # PRKS

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_ignores_disabled(self):
        """filter_tables ignore les tables désactivées"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'enable': True, 'primary_key': 'id', 'delta': ''},
            {'table_name': 'PRKS', 'enable': False, 'primary_key': 'code', 'delta': ''},
            {'table_name': 'COST', 'primary_key': 'id', 'delta': ''}
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
        table_names = [t['table_name'] for t in result]
        assert 'CSKS' in table_names
        assert 'COST' in table_names
        assert 'PRKS' not in table_names

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_disabled_not_checked_in_status(self):
        """Tables désactivées ne sont pas vérifiées dans le statut API"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'enable': True, 'primary_key': 'id', 'delta': ''},
            {'table_name': 'UNKNOWN', 'enable': False}  # Désactivée, ne devrait pas causer d'erreur
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {
            'CSKS': {'status': 'OK', 'mode': 'FULL'}
            # UNKNOWN n'existe pas mais c'est OK car désactivée
        }

        # Ne devrait pas lever d'exception
        result = filter_obj.filter_tables(current_status)

        assert len(result) == 1
        assert result[0]['table_name'] == 'CSKS'

    @patch('amue.operators.table_management.table_filter.NotificationService', None)
    def test_filter_tables_all_disabled(self):
        """Toutes les tables désactivées retourne liste vide"""
        from amue.operators.table_management.table_filter import AMUETableFilter

        tables_config = [
            {'table_name': 'CSKS', 'enable': False},
            {'table_name': 'PRKS', 'enable': False}
        ]

        filter_obj = AMUETableFilter(tables_config=tables_config)

        current_status = {}

        result = filter_obj.filter_tables(current_status)

        assert len(result) == 0
