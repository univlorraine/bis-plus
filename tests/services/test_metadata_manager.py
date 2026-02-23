"""
Tests unitaires pour AMUEMetadataManager
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestMetadataManagerInit:
    """Tests pour l'initialisation de AMUEMetadataManager"""

    def test_init(self):
        """Initialisation correcte"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        assert manager.MAX_RETRIES == 3
        assert manager.RETRY_DELAY_SECONDS == 2


class TestMetadataManagerUpdateMetadata:
    """Tests pour update_metadata"""

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_success(self, mock_tcm_cls):
        """Mise à jour réussie"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'name': 'CSKS', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''}
        ]

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {
                'table_name': 'csks',
                'status': 'success',
                'fingerprint_API': 'new_api_fp_123',
                'fingerprint_UL': 'new_ul_fp_456',
                'primary_keys': 'id'
            }
        ]

        manager.update_metadata(import_results)

        # Vérifie que save_tables_config a été appelé
        assert mock_tcm.save_tables_config.called

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_empty_results(self, mock_tcm_cls):
        """Résultats vides ne font rien"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        manager.update_metadata([])

        # Aucun appel à get_tables_config
        mock_tcm.get_tables_config.assert_not_called()

    @patch('amue.services.metadata_manager.time.sleep')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_retry_on_error(self, mock_tcm_cls, mock_sleep):
        """Retry en cas d'erreur de sauvegarde"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'name': 'CSKS', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''}
        ]
        # Premier save échoue, deuxième réussit
        mock_tcm.save_tables_config.side_effect = [ValueError("test"), None]

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {'table_name': 'csks', 'status': 'success', 'fingerprint_API': 'fp_api', 'fingerprint_UL': 'fp_ul'}
        ]

        manager.update_metadata(import_results)

        # Sleep appelé entre les tentatives
        assert mock_sleep.called

    @patch('amue.services.metadata_manager.time.sleep')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_fail_after_retries(self, mock_tcm_cls, mock_sleep):
        """Échec après tous les retries"""
        from airflow.exceptions import AirflowException

        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.side_effect = Exception("DB connection failed")

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {'table_name': 'csks', 'status': 'success', 'fingerprint_API': 'fp_api', 'fingerprint_UL': 'fp_ul'}
        ]

        with pytest.raises(AirflowException, match="Impossible de sauvegarder"):
            manager.update_metadata(import_results)

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_saves_report_start(self, mock_tcm_cls, mock_admin_cls):
        """update_metadata sauvegarde le report_start via AdminStateManager"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'name': 'CSKS', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''}
        ]

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {'table_name': 'csks', 'status': 'success', 'fingerprint_API': 'fp', 'fingerprint_UL': 'fp'}
        ]

        manager.update_metadata(import_results, report_start='2026-02-17T10:08:19+00:00')

        mock_admin.set_last_report_start.assert_called_with('2026-02-17T10:08:19+00:00')


class TestMetadataManagerShouldUpdate:
    """Tests pour _should_update_metadata"""

    def test_should_update_success(self):
        """Mise à jour si statut success"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        result = {'table_name': 'CSKS', 'status': 'success'}

        assert manager._should_update_metadata(result) is True

    def test_should_not_update_error(self):
        """Pas de mise à jour si statut error"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        result = {'table_name': 'CSKS', 'status': 'error'}

        assert manager._should_update_metadata(result) is False

    def test_should_not_update_no_table_name(self):
        """Pas de mise à jour sans nom de table"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        result = {'status': 'success'}

        assert manager._should_update_metadata(result) is False


class TestMetadataManagerLoadConfig:
    """Tests pour _load_tables_config"""

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_load_tables_config(self, mock_tcm_cls):
        """Charge la config depuis la BDD"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'name': 'CSKS', 'fingerprint_API': 'fp1', 'fingerprint_UL': 'fp2',
             'primary_key': '', 'delta': '', 'enable': True}
        ]

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        config = manager._load_tables_config()

        assert len(config) == 1
        assert config[0]['name'] == 'CSKS'

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_load_tables_config_raises_on_error(self, mock_tcm_cls):
        """Exception propagée si chargement échoue"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.side_effect = Exception("DB error")

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        with pytest.raises(Exception, match="DB error"):
            manager._load_tables_config()


class TestMetadataManagerUpdateTable:
    """Tests pour _update_table_metadata"""

    def test_update_table_found(self):
        """Met à jour une table existante (fingerprints et PKs)"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        manager._report_start = '2024-01-15T10:08:19+00:00'

        tables_config = [
            {'name': 'CSKS', 'fingerprint_API': 'old_api', 'fingerprint_UL': 'old_ul', 'primary_key': ''}
        ]

        result = {
            'table_name': 'CSKS',
            'fingerprint_API': 'new_api',
            'fingerprint_UL': 'new_ul',
            'primary_keys': 'id'
        }

        updated = manager._update_table_metadata(tables_config, result)

        assert updated is True
        assert tables_config[0]['fingerprint_API'] == 'new_api'
        assert tables_config[0]['fingerprint_UL'] == 'new_ul'
        assert 'finger_print' not in tables_config[0]
        assert tables_config[0]['primary_key'] == 'id'
        # last_import n'est plus stocké par table
        assert 'last_import' not in tables_config[0]

    def test_update_table_no_last_import_stored(self):
        """last_import n'est plus stocké par table — le timestamp global est dans amue_state"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        manager._report_start = '2026-02-17T10:08:19+00:00'

        tables_config = [
            {'name': 'CSKS', 'fingerprint_API': 'old_api', 'fingerprint_UL': 'old_ul', 'primary_key': 'id'}
        ]

        result = {
            'table_name': 'CSKS',
            'fingerprint_API': 'new_api',
            'fingerprint_UL': 'new_ul'
        }

        manager._update_table_metadata(tables_config, result)

        assert 'last_import' not in tables_config[0]

    def test_update_table_not_found(self):
        """Table non trouvée retourne False"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        tables_config = [
            {'name': 'CSKS', 'fingerprint_API': 'old_api', 'fingerprint_UL': 'old_ul'}
        ]

        result = {
            'table_name': 'UNKNOWN',
            'fingerprint_API': 'new_api',
            'fingerprint_UL': 'new_ul'
        }

        updated = manager._update_table_metadata(tables_config, result)

        assert updated is False


class TestMetadataManagerGetLastSuccess:
    """Tests pour get_last_success_date"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_get_last_success_date_valid(self, mock_admin_cls):
        """Récupère une date valide"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_successful_run.return_value = '2024-01-15T10:30:00'

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_get_last_success_date_empty(self, mock_admin_cls):
        """Retourne None si vide"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_successful_run.return_value = None

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is None

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_get_last_success_date_invalid(self, mock_admin_cls):
        """Retourne None si date invalide"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_successful_run.return_value = 'invalid-date'

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is None


class TestMetadataManagerGetTableMetadata:
    """Tests pour get_table_metadata"""

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_get_table_metadata_found(self, mock_tcm_cls):
        """Récupère les métadonnées d'une table (sans last_import)"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_table_metadata.return_value = {
            'name': 'CSKS',
            'fingerprint_API': 'fp_api_123',
            'fingerprint_UL': 'fp_ul_456',
            'primary_key': 'id',
            'delta': 'date_modif'
        }

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_table_metadata('csks')  # Minuscule

        assert result is not None
        assert result.name == 'CSKS'
        assert result.fingerprint_API == 'fp_api_123'
        assert result.fingerprint_UL == 'fp_ul_456'
        assert result.primary_key == 'id'
        assert result.delta == 'date_modif'

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_get_table_metadata_not_found(self, mock_tcm_cls):
        """Retourne None si table non trouvée"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_table_metadata.return_value = None

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_table_metadata('UNKNOWN')

        assert result is None


class TestMetadataManagerResetTable:
    """Tests pour reset_table_metadata"""

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_reset_table_metadata_success(self, mock_tcm_cls):
        """Réinitialise les métadonnées d'une table"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.reset_table_metadata.return_value = True

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('CSKS')

        assert result is True
        mock_tcm.reset_table_metadata.assert_called_once_with('CSKS')

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_reset_table_metadata_not_found(self, mock_tcm_cls):
        """Retourne False si table non trouvée"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.reset_table_metadata.return_value = False

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('UNKNOWN')

        assert result is False

    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_reset_table_metadata_error(self, mock_tcm_cls):
        """Retourne False en cas d'erreur"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.reset_table_metadata.return_value = False

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('CSKS')

        assert result is False


class TestMetadataManagerFinishTimestamp:
    """Tests pour la sauvegarde du finish timestamp"""

    @patch('amue.services.admin_state_manager.AdminStateManager')
    def test_save_finish_timestamp(self, mock_admin_cls):
        """Sauvegarde du finish timestamp via AdminStateManager"""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        manager._save_finish_timestamp('2024-01-15T10:00:00')

        mock_admin.set_last_finish_timestamp.assert_called_once_with('2024-01-15T10:00:00')

    @patch('amue.services.admin_state_manager.AdminStateManager')
    @patch('amue.services.table_config_manager.TableConfigManager')
    def test_update_metadata_with_finish_timestamp(self, mock_tcm_cls, mock_admin_cls):
        """update_metadata sauvegarde le finish_timestamp via AdminStateManager"""
        mock_tcm = MagicMock()
        mock_tcm_cls.return_value = mock_tcm
        mock_tcm.get_tables_config.return_value = [
            {'name': 'CSKS', 'fingerprint_API': '', 'fingerprint_UL': '', 'primary_key': ''}
        ]

        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.get_last_finish_timestamp.return_value = None

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {
                'table_name': 'csks',
                'status': 'success',
                'fingerprint_API': 'new_api_fp_123',
                'fingerprint_UL': 'new_ul_fp_456'
            }
        ]

        manager.update_metadata(import_results, finish_timestamp='2024-01-15T10:00:00')

        mock_admin.set_last_finish_timestamp.assert_called_with('2024-01-15T10:00:00')


class TestTableMetadata:
    """Tests pour TableMetadata dataclass"""

    def test_table_metadata(self):
        """Création de TableMetadata (sans last_import)"""
        from amue.services.metadata_manager import TableMetadata

        metadata = TableMetadata(
            name='CSKS',
            fingerprint_API='fp_api_123',
            fingerprint_UL='fp_ul_456',
            primary_key='id',
            delta='date_modif'
        )

        assert metadata.name == 'CSKS'
        assert metadata.fingerprint_API == 'fp_api_123'
        assert metadata.fingerprint_UL == 'fp_ul_456'
        assert metadata.primary_key == 'id'
        assert metadata.delta == 'date_modif'

    def test_table_metadata_defaults(self):
        """Valeurs par défaut de TableMetadata"""
        from amue.services.metadata_manager import TableMetadata

        metadata = TableMetadata(
            name='CSKS',
            fingerprint_API='fp_api_123',
            fingerprint_UL='fp_ul_456'
        )

        assert metadata.primary_key == ''
        assert metadata.delta == ''
