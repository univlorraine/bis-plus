"""
Tests unitaires pour FinishTimestampValidator.
"""
import pytest
from unittest.mock import patch


class TestFinishTimestampValidatorValidate:
    """Tests pour validate()."""

    def test_validate_iso_timestamp_valid(self):
        """Retourne True pour un timestamp ISO 8601 valide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("2026-03-09T10:00:00") is True

    def test_validate_timestamp_with_space_separator_valid(self):
        """Retourne True pour un timestamp avec espace comme séparateur."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("2026-03-09 10:00:00") is True

    def test_validate_timestamp_with_timezone_valid(self):
        """Retourne True pour un timestamp avec timezone +00:00."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("2026-03-09T10:00:00+00:00") is True

    def test_validate_empty_string_invalid(self):
        """Retourne False pour une chaîne vide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("") is False

    def test_validate_whitespace_only_invalid(self):
        """Retourne False pour une chaîne d'espaces."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("   ") is False

    def test_validate_null_string_invalid(self):
        """Retourne False pour la valeur textuelle 'null'."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("null") is False

    def test_validate_none_string_invalid(self):
        """Retourne False pour la valeur textuelle 'none'."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("none") is False

    def test_validate_zero_string_invalid(self):
        """Retourne False pour la valeur '0'."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("0") is False

    def test_validate_undefined_string_invalid(self):
        """Retourne False pour la valeur 'undefined'."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("undefined") is False

    def test_validate_all_zeros_invalid(self):
        """Retourne False pour '00000000'."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("00000000") is False

    def test_validate_case_insensitive(self):
        """La détection des valeurs invalides est insensible à la casse."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        validator = FinishTimestampValidator()
        assert validator.validate("NULL") is False
        assert validator.validate("None") is False
        assert validator.validate("UNDEFINED") is False


class TestFinishTimestampValidatorNormalizeTs:
    """Tests pour _normalize_ts() (méthode statique interne)."""

    def test_normalize_ts_iso_with_T(self):
        """Normalise un timestamp ISO avec séparateur T."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        result = FinishTimestampValidator._normalize_ts("2026-03-09T10:00:00")
        assert result == "2026-03-09T10:00:00"

    def test_normalize_ts_with_space(self):
        """Remplace l'espace par T."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        result = FinishTimestampValidator._normalize_ts("2026-03-09 10:00:00")
        assert result == "2026-03-09T10:00:00"

    def test_normalize_ts_strips_microseconds(self):
        """Supprime les microsecondes."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        result = FinishTimestampValidator._normalize_ts("2026-03-09T10:00:00.123456")
        assert result == "2026-03-09T10:00:00"

    def test_normalize_ts_converts_to_utc(self):
        """Convertit en UTC si timezone présente."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        # +00:00 → pas de changement d'heure
        result = FinishTimestampValidator._normalize_ts("2026-03-09T10:00:00+00:00")
        assert result == "2026-03-09T10:00:00"

    def test_normalize_ts_empty_returns_empty(self):
        """Retourne '' pour une chaîne vide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        result = FinishTimestampValidator._normalize_ts("")
        assert result == ""


class TestFinishTimestampValidatorShouldSkip:
    """Tests pour should_skip()."""

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_false_on_first_execution(self, MockAdmin):
        """Retourne False (import exécuté) si aucun timestamp stocké."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = None

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-09T10:00:00")

        assert result is False

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_false_when_new_timestamp(self, MockAdmin):
        """Retourne False si le timestamp actuel est supérieur au stocké."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = "2026-03-08T10:00:00"

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-09T10:00:00")

        assert result is False

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_true_when_same_timestamp(self, MockAdmin):
        """Retourne True si le timestamp est identique au stocké."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = "2026-03-09T10:00:00"

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-09T10:00:00")

        assert result is True

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_true_when_older_timestamp(self, MockAdmin):
        """Retourne True si le timestamp actuel est inférieur au stocké (cas anormal)."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = "2026-03-09T10:00:00"

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-08T10:00:00")

        assert result is True

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_false_when_invalid_finish(self, MockAdmin):
        """Retourne False (import exécuté par précaution) si le finish est invalide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = "2026-03-09T10:00:00"

        validator = FinishTimestampValidator()
        result = validator.should_skip("")

        assert result is False
        # get_last_finish_timestamp ne doit pas être appelé si le finish est invalide
        MockAdmin.return_value.get_last_finish_timestamp.assert_not_called()

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_false_when_stored_empty_string(self, MockAdmin):
        """Retourne False si le timestamp stocké est une chaîne vide."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        MockAdmin.return_value.get_last_finish_timestamp.return_value = ""

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-09T10:00:00")

        assert result is False

    @patch('common.services.admin_state_manager.AdminStateManager')
    def test_should_skip_normalizes_timestamps_for_comparison(self, MockAdmin):
        """Normalise les deux timestamps avant comparaison (espace vs T)."""
        from amue.services.api.finish_timestamp_validator import FinishTimestampValidator

        # Stocké avec T, API avec espace → même instant
        MockAdmin.return_value.get_last_finish_timestamp.return_value = "2026-03-09T10:00:00"

        validator = FinishTimestampValidator()
        result = validator.should_skip("2026-03-09 10:00:00")

        assert result is True
