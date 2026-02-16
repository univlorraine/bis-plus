# tests/utils/config/test_airflow_helpers.py
"""
Tests unitaires pour AirflowVariableManager.

Teste les vrais chemins du code : SDK d'abord, fallback models, default.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

from amue.utils.config.airflow_helpers import AirflowVariableManager


class TestAirflowVariableManagerGet:
    """Tests pour AirflowVariableManager.get - vrais chemins de code."""

    def test_get_via_sdk_success(self):
        """get() utilise SDK Variable quand disponible."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.get.return_value = "sdk_value"

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            result = AirflowVariableManager.get("test_key")

        assert result == "sdk_value"
        mock_sdk_module.Variable.get.assert_called_once_with("test_key", default=None)

    def test_get_via_sdk_with_default(self):
        """get() passe le default au SDK."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.get.return_value = "fallback"

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            result = AirflowVariableManager.get("missing", default="fallback")

        assert result == "fallback"
        mock_sdk_module.Variable.get.assert_called_once_with("missing", default="fallback")

    def test_get_fallback_to_models_on_import_error(self):
        """get() fallback vers models si SDK ImportError."""
        mock_models_module = MagicMock()
        mock_models_module.Variable.get.return_value = "models_value"

        with patch.dict('sys.modules', {'airflow.sdk': None}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                result = AirflowVariableManager.get("test_key")

        assert result == "models_value"
        mock_models_module.Variable.get.assert_called_once_with("test_key", default_var=None)

    def test_get_fallback_to_models_on_key_error(self):
        """get() fallback vers models si SDK lève KeyError."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.get.side_effect = KeyError("not found")
        mock_models_module = MagicMock()
        mock_models_module.Variable.get.return_value = "models_value"

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                result = AirflowVariableManager.get("test_key")

        assert result == "models_value"

    def test_get_returns_default_when_both_fail(self):
        """get() retourne default si SDK et models échouent."""
        with patch.dict('sys.modules', {'airflow.sdk': None, 'airflow.models': None}):
            result = AirflowVariableManager.get("missing", default="my_default")

        assert result == "my_default"

    def test_get_returns_none_default(self):
        """get() retourne None par défaut si rien ne fonctionne."""
        with patch.dict('sys.modules', {'airflow.sdk': None, 'airflow.models': None}):
            result = AirflowVariableManager.get("missing")

        assert result is None

    def test_get_models_uses_default_var_parameter(self):
        """get() utilise default_var= pour le fallback models (Airflow 2.x)."""
        mock_models_module = MagicMock()
        mock_models_module.Variable.get.return_value = "val"

        with patch.dict('sys.modules', {'airflow.sdk': None}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                AirflowVariableManager.get("key", default="def")

        mock_models_module.Variable.get.assert_called_once_with("key", default_var="def")


class TestAirflowVariableManagerSet:
    """Tests pour AirflowVariableManager.set - vrais chemins de code."""

    def test_set_string_via_sdk(self):
        """set() string via SDK."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock()
        mock_sdk_module.Variable.set.return_value = None

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            result = AirflowVariableManager.set("key", "value")

        assert result is True
        mock_sdk_module.Variable.set.assert_called_once_with("key", "value", None)

    def test_set_dict_serializes_to_json(self):
        """set() sérialise un dict en JSON avant stockage."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock()

        test_dict = {"nested": {"a": 1}, "list": [1, 2]}

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            result = AirflowVariableManager.set("key", test_dict)

        assert result is True
        # Vérifie que la valeur passée est bien du JSON
        actual_value = mock_sdk_module.Variable.set.call_args[0][1]
        parsed = json.loads(actual_value)
        assert parsed == test_dict

    def test_set_list_serializes_to_json(self):
        """set() sérialise une liste en JSON."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock()

        test_list = [1, "two", {"three": 3}]

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            AirflowVariableManager.set("key", test_list)

        actual_value = mock_sdk_module.Variable.set.call_args[0][1]
        assert json.loads(actual_value) == test_list

    def test_set_fallback_to_models_when_sdk_has_no_set(self):
        """set() fallback vers models si SDK Variable n'a pas set()."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable = MagicMock(spec=[])  # Pas de méthode set

        mock_models_module = MagicMock()
        mock_models_module.Variable.set = MagicMock()

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                result = AirflowVariableManager.set("key", "value")

        assert result is True
        mock_models_module.Variable.set.assert_called_once_with("key", "value", description=None)

    def test_set_fallback_to_models_on_sdk_import_error(self):
        """set() fallback vers models si SDK non disponible."""
        mock_models_module = MagicMock()
        mock_models_module.Variable.set = MagicMock()

        with patch.dict('sys.modules', {'airflow.sdk': None}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                result = AirflowVariableManager.set("key", "value")

        assert result is True

    def test_set_returns_false_when_both_fail(self):
        """set() retourne False si SDK et models échouent."""
        with patch.dict('sys.modules', {'airflow.sdk': None, 'airflow.models': None}):
            result = AirflowVariableManager.set("key", "value")

        assert result is False

    def test_set_with_description(self):
        """set() passe la description au SDK."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock()

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            AirflowVariableManager.set("key", "value", description="My var")

        mock_sdk_module.Variable.set.assert_called_once_with("key", "value", "My var")

    def test_set_sdk_exception_falls_through_to_models(self):
        """set() fallback models si SDK lève une exception."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock(side_effect=RuntimeError("SDK error"))

        mock_models_module = MagicMock()
        mock_models_module.Variable.set = MagicMock()

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            with patch.dict('sys.modules', {'airflow.models': mock_models_module}):
                result = AirflowVariableManager.set("key", "value")

        assert result is True
        mock_models_module.Variable.set.assert_called_once()

    def test_set_int_serializes_to_json(self):
        """set() sérialise un int en JSON."""
        mock_sdk_module = MagicMock()
        mock_sdk_module.Variable.set = MagicMock()

        with patch.dict('sys.modules', {'airflow.sdk': mock_sdk_module}):
            AirflowVariableManager.set("key", 42)

        actual_value = mock_sdk_module.Variable.set.call_args[0][1]
        assert actual_value == "42"
