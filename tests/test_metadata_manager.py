"""
Tests unitaires pour AMUEMetadataManager
"""
import pytest
import sys
import os
import json
from unittest.mock import MagicMock, patch
from datetime import datetime

# Ajoute le chemin des plugins au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))


class TestMetadataManagerInit:
    """Tests pour l'initialisation de AMUEMetadataManager"""

    def test_init(self):
        """Initialisation correcte"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        assert manager.tables_var_name == 'amue_tables_to_import'
        assert manager.last_success_var_name == 'amue_last_successful_run'
        assert manager.last_finish_var_name == 'amue_last_finish_timestamp'
        assert manager.MAX_RETRIES == 3
        assert manager.RETRY_DELAY_SECONDS == 2


class TestMetadataManagerUpdateMetadata:
    """Tests pour update_metadata"""

    @patch('amue.services.metadata_manager.VarMgr')
    def test_update_metadata_success(self, mock_varmgr):
        """Mise à jour réussie"""
        tables_config = json.dumps([
            {'name': 'CSKS', 'finger_print': '', 'last_import': '', 'primary_key': ''}
        ])
        mock_varmgr.get.return_value = tables_config
        mock_varmgr.set.return_value = True

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {
                'table_name': 'csks',
                'status': 'success',
                'finger_print': 'new_fingerprint_123',
                'primary_keys': 'id'
            }
        ]

        manager.update_metadata(import_results)

        # Vérifie que set a été appelé
        assert mock_varmgr.set.called

    @patch('amue.services.metadata_manager.VarMgr')
    def test_update_metadata_empty_results(self, mock_varmgr):
        """Résultats vides ne font rien"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        manager.update_metadata([])

        # Aucun appel à get/set
        mock_varmgr.get.assert_not_called()

    @patch('amue.services.metadata_manager.time.sleep')
    @patch('amue.services.metadata_manager.VarMgr')
    def test_update_metadata_retry_on_error(self, mock_varmgr, mock_sleep):
        """Retry en cas d'erreur de sauvegarde"""
        from airflow.exceptions import AirflowException

        # Config valide, mais sauvegarde échoue puis réussit
        tables_config = json.dumps([
            {'name': 'CSKS', 'finger_print': '', 'last_import': '', 'primary_key': ''}
        ])
        mock_varmgr.get.return_value = tables_config
        # Premier set échoue (ValueError), deuxième réussit
        mock_varmgr.set.side_effect = [ValueError("test"), True, True]

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {'table_name': 'csks', 'status': 'success', 'finger_print': 'fp'}
        ]

        manager.update_metadata(import_results)

        # Sleep appelé entre les tentatives
        assert mock_sleep.called

    @patch('amue.services.metadata_manager.time.sleep')
    @patch('amue.services.metadata_manager.VarMgr')
    def test_update_metadata_fail_after_retries(self, mock_varmgr, mock_sleep):
        """Échec après tous les retries - erreur de chargement"""
        from airflow.exceptions import AirflowException

        mock_varmgr.get.side_effect = json.JSONDecodeError("test", "test", 0)

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {'table_name': 'csks', 'status': 'success', 'finger_print': 'fp'}
        ]

        with pytest.raises(AirflowException, match="Impossible de charger"):
            manager.update_metadata(import_results)


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

    @patch('amue.services.metadata_manager.VarMgr')
    def test_load_tables_config_string(self, mock_varmgr):
        """Charge une config JSON string"""
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'finger_print': 'fp1'}
        ])

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        config = manager._load_tables_config()

        assert len(config) == 1
        assert config[0]['name'] == 'CSKS'

    @patch('amue.services.metadata_manager.VarMgr')
    def test_load_tables_config_list(self, mock_varmgr):
        """Charge une config déjà en liste"""
        mock_varmgr.get.return_value = [
            {'name': 'CSKS', 'finger_print': 'fp1'}
        ]

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        config = manager._load_tables_config()

        assert len(config) == 1

    @patch('amue.services.metadata_manager.VarMgr')
    def test_load_tables_config_invalid(self, mock_varmgr):
        """Config invalide lève une erreur"""
        from airflow.exceptions import AirflowException

        mock_varmgr.get.return_value = "not a valid json"

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        with pytest.raises(AirflowException, match="Impossible de charger"):
            manager._load_tables_config()


class TestMetadataManagerUpdateTable:
    """Tests pour _update_table_metadata"""

    def test_update_table_found(self):
        """Met à jour une table existante"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        tables_config = [
            {'name': 'CSKS', 'finger_print': 'old_fp', 'primary_key': ''}
        ]

        result = {
            'table_name': 'CSKS',
            'finger_print': 'new_fp',
            'primary_keys': 'id'
        }

        updated = manager._update_table_metadata(tables_config, result)

        assert updated is True
        assert tables_config[0]['finger_print'] == 'new_fp'
        assert tables_config[0]['primary_key'] == 'id'
        assert 'last_import' in tables_config[0]

    def test_update_table_not_found(self):
        """Table non trouvée retourne False"""
        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        tables_config = [
            {'name': 'CSKS', 'finger_print': 'old_fp'}
        ]

        result = {
            'table_name': 'UNKNOWN',
            'finger_print': 'new_fp'
        }

        updated = manager._update_table_metadata(tables_config, result)

        assert updated is False


class TestMetadataManagerGetLastSuccess:
    """Tests pour get_last_success_date"""

    @patch('amue.services.metadata_manager.VarMgr')
    def test_get_last_success_date_valid(self, mock_varmgr):
        """Récupère une date valide"""
        mock_varmgr.get.return_value = '2024-01-15T10:30:00'

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    @patch('amue.services.metadata_manager.VarMgr')
    def test_get_last_success_date_empty(self, mock_varmgr):
        """Retourne None si vide"""
        mock_varmgr.get.return_value = ''

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is None

    @patch('amue.services.metadata_manager.VarMgr')
    def test_get_last_success_date_invalid(self, mock_varmgr):
        """Retourne None si date invalide"""
        mock_varmgr.get.return_value = 'invalid-date'

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_last_success_date()

        assert result is None


class TestMetadataManagerGetTableMetadata:
    """Tests pour get_table_metadata"""

    @patch('amue.services.metadata_manager.VarMgr')
    def test_get_table_metadata_found(self, mock_varmgr):
        """Récupère les métadonnées d'une table"""
        mock_varmgr.get.return_value = json.dumps([
            {
                'name': 'CSKS',
                'finger_print': 'fp123',
                'last_import': '2024-01-15',
                'primary_key': 'id',
                'delta': 'date_modif'
            }
        ])

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_table_metadata('csks')  # Minuscule

        assert result is not None
        assert result.name == 'CSKS'
        assert result.finger_print == 'fp123'
        assert result.last_import == '2024-01-15'
        assert result.primary_key == 'id'
        assert result.delta == 'date_modif'

    @patch('amue.services.metadata_manager.VarMgr')
    def test_get_table_metadata_not_found(self, mock_varmgr):
        """Retourne None si table non trouvée"""
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'finger_print': 'fp123'}
        ])

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.get_table_metadata('UNKNOWN')

        assert result is None


class TestMetadataManagerResetTable:
    """Tests pour reset_table_metadata"""

    @patch('amue.services.metadata_manager.VarMgr')
    def test_reset_table_metadata_success(self, mock_varmgr):
        """Réinitialise les métadonnées d'une table"""
        mock_varmgr.get.return_value = json.dumps([
            {
                'name': 'CSKS',
                'finger_print': 'fp123',
                'last_import': '2024-01-15'
            }
        ])
        mock_varmgr.set.return_value = True

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('CSKS')

        assert result is True
        # Vérifie que set a été appelé avec les métadonnées réinitialisées
        call_args = mock_varmgr.set.call_args[0]
        saved_config = json.loads(call_args[1])
        assert saved_config[0]['finger_print'] == ''
        assert saved_config[0]['last_import'] == ''

    @patch('amue.services.metadata_manager.VarMgr')
    def test_reset_table_metadata_not_found(self, mock_varmgr):
        """Retourne False si table non trouvée"""
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'finger_print': 'fp123'}
        ])

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('UNKNOWN')

        assert result is False

    @patch('amue.services.metadata_manager.VarMgr')
    def test_reset_table_metadata_error(self, mock_varmgr):
        """Retourne False en cas d'erreur de sauvegarde"""
        # Config valide mais sauvegarde échoue
        mock_varmgr.get.return_value = json.dumps([
            {'name': 'CSKS', 'finger_print': 'fp123'}
        ])
        mock_varmgr.set.side_effect = ValueError("test")

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        result = manager.reset_table_metadata('CSKS')

        assert result is False


class TestMetadataManagerFinishTimestamp:
    """Tests pour la sauvegarde du finish timestamp"""

    @patch('amue.services.metadata_manager.VarMgr')
    def test_save_finish_timestamp(self, mock_varmgr):
        """Sauvegarde du finish timestamp"""
        mock_varmgr.get.return_value = ''
        mock_varmgr.set.return_value = True

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()
        manager._save_finish_timestamp('2024-01-15T10:00:00')

        # Vérifie que set a été appelé avec la bonne variable
        mock_varmgr.set.assert_called_with('amue_last_finish_timestamp', '2024-01-15T10:00:00')

    @patch('amue.services.metadata_manager.VarMgr')
    def test_update_metadata_with_finish_timestamp(self, mock_varmgr):
        """update_metadata sauvegarde le finish_timestamp"""
        tables_config = json.dumps([
            {'name': 'CSKS', 'finger_print': '', 'last_import': '', 'primary_key': ''}
        ])
        mock_varmgr.get.side_effect = lambda key, default=None: {
            'amue_tables_to_import': tables_config,
            'amue_last_finish_timestamp': ''
        }.get(key, default) if key != 'amue_tables_to_import' else tables_config
        mock_varmgr.set.return_value = True

        from amue.services.metadata_manager import AMUEMetadataManager

        manager = AMUEMetadataManager()

        import_results = [
            {
                'table_name': 'csks',
                'status': 'success',
                'finger_print': 'new_fingerprint_123'
            }
        ]

        manager.update_metadata(import_results, finish_timestamp='2024-01-15T10:00:00')

        # Vérifie que set a été appelé pour le finish timestamp
        calls = mock_varmgr.set.call_args_list
        finish_call = [c for c in calls if 'amue_last_finish_timestamp' in str(c)]
        assert len(finish_call) > 0


class TestTableMetadata:
    """Tests pour TableMetadata dataclass"""

    def test_table_metadata(self):
        """Création de TableMetadata"""
        from amue.services.metadata_manager import TableMetadata

        metadata = TableMetadata(
            name='CSKS',
            finger_print='fp123',
            last_import='2024-01-15',
            primary_key='id',
            delta='date_modif'
        )

        assert metadata.name == 'CSKS'
        assert metadata.finger_print == 'fp123'
        assert metadata.last_import == '2024-01-15'
        assert metadata.primary_key == 'id'
        assert metadata.delta == 'date_modif'

    def test_table_metadata_defaults(self):
        """Valeurs par défaut de TableMetadata"""
        from amue.services.metadata_manager import TableMetadata

        metadata = TableMetadata(
            name='CSKS',
            finger_print='fp123',
            last_import='2024-01-15'
        )

        assert metadata.primary_key == ''
        assert metadata.delta == ''
