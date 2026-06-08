"""
Tests unitaires pour ImportConfigValidator.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestImportConfigValidatorGetPrimaryKeys:
    """Tests pour ImportConfigValidator.get_primary_keys()."""

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_nominal(self, MockTCM):
        """Retourne la liste des PKs configurées pour une table connue."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "table_name": "csks",
            "primary_key": "bukrs, kostl",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == ["bukrs", "kostl"]
        MockTCM.return_value.get_table_metadata.assert_called_once_with("CSKS")

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_lowercased(self, MockTCM):
        """Les PKs sont retournées en minuscules."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "primary_key": "BUKRS, KOSTL",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == ["bukrs", "kostl"]

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_single_pk(self, MockTCM):
        """Fonctionne avec une seule colonne PK."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "primary_key": "mandt",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("T001")

        assert result == ["mandt"]

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_table_not_found(self, MockTCM):
        """Retourne [] si la table est absente de la config."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = None

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("TABLE_INCONNUE")

        assert result == []

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_empty_primary_key(self, MockTCM):
        """Retourne [] si primary_key est une chaîne vide."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "table_name": "csks",
            "primary_key": "",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == []

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_missing_primary_key_field(self, MockTCM):
        """Retourne [] si la clé 'primary_key' est absente des métadonnées."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "table_name": "csks",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == []

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_db_error_propagates(self, MockTCM):
        """Une erreur DB (psycopg2.Error) est propagée — un import sans PK serait dangereux."""
        from psycopg2 import OperationalError

        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.side_effect = OperationalError("DB down")

        validator = ImportConfigValidator()
        with pytest.raises(OperationalError):
            validator.get_primary_keys("CSKS")

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_invalid_config_returns_empty(self, MockTCM):
        """Une erreur de format de config (KeyError/AttributeError) retourne []."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        # Objet sans `.get()` → AttributeError dans le bloc try interne
        MockTCM.return_value.get_table_metadata.return_value = "not_a_dict"

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == []

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_strips_whitespace(self, MockTCM):
        """Les espaces autour des noms de colonnes sont supprimés."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "primary_key": "  bukrs ,  kostl  ",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == ["bukrs", "kostl"]

    @patch('amue.operators.pipeline.import_config_validator.TableConfigManager')
    def test_get_primary_keys_ignores_empty_segments(self, MockTCM):
        """Les segments vides (double virgule) sont ignorés."""
        from amue.operators.pipeline.import_config_validator import ImportConfigValidator

        MockTCM.return_value.get_table_metadata.return_value = {
            "primary_key": "bukrs,,kostl",
        }

        validator = ImportConfigValidator()
        result = validator.get_primary_keys("CSKS")

        assert result == ["bukrs", "kostl"]
