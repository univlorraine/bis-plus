# tests/operators/pipeline/test_data_streamer.py
"""
Tests unitaires pour AMUEDataStreamer.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from airflow.exceptions import AirflowException

from amue.operators.pipeline.data_streamer import AMUEDataStreamer
from common.services.retry_service import ErrorCategory, RetryResult


class TestAMUEDataStreamerInit:
    """Tests d'initialisation du streamer."""

    def test_init_stores_api_hook(self):
        """Test que le hook API est stocké."""
        mock_hook = Mock()
        endpoint = "https://api.example.com/data"

        streamer = AMUEDataStreamer(mock_hook, endpoint)

        assert streamer.api_hook is mock_hook

    def test_init_stores_endpoint(self):
        """Test que l'endpoint est stocké."""
        mock_hook = Mock()
        endpoint = "https://api.example.com/data"

        streamer = AMUEDataStreamer(mock_hook, endpoint)

        assert streamer.endpoint == endpoint


class TestBuildQueryParams:
    """Tests pour _build_query_params."""

    def test_full_import_basic_params(self):
        """Test paramètres de base pour import full."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        import_config = {"import_type": "full"}

        params = streamer._build_query_params("CSKS", import_config)

        assert params["nom"] == "CSKS"
        assert params["f"] == "json"
        assert "q" not in params

    def test_differential_import_with_delta(self):
        """Test import différentiel avec delta."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        import_config = {
            "import_type": "delta",
            "delta": "AEDAT",
            "last_import": "2024-01-15T10:30:00"
        }

        params = streamer._build_query_params("CSKS", import_config)

        assert params["nom"] == "CSKS"
        assert "q" in params
        assert "AEDAT>='20240115'" in params["q"]

    def test_differential_without_delta_column(self):
        """Test import différentiel sans colonne delta - mode full."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        import_config = {
            "import_type": "delta",
            "delta": "",
            "last_import": "2024-01-15"
        }

        params = streamer._build_query_params("PRKS", import_config)

        assert "q" not in params

    def test_differential_without_last_import(self):
        """Test import différentiel sans date de dernier import - mode full."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        import_config = {
            "import_type": "delta",
            "delta": "AEDAT",
            "last_import": ""
        }

        params = streamer._build_query_params("PRKS", import_config)

        assert "q" not in params

    def test_table_name_uppercased(self):
        """Test que le nom de table est en majuscules."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        params = streamer._build_query_params("csks", {"import_type": "full"})

        assert params["nom"] == "CSKS"


class TestFormatDateForQuery:
    """Tests pour _format_date_for_query."""

    def test_iso_format_with_time(self):
        """Test format ISO avec heure."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        result = streamer._format_date_for_query("2024-01-15T10:30:00")

        assert result == "20240115"

    def test_iso_format_with_z_suffix(self):
        """Test format ISO avec suffixe Z."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        result = streamer._format_date_for_query("2024-01-15T10:30:00Z")

        assert result == "20240115"

    def test_iso_format_date_only(self):
        """Test format ISO date seule."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        result = streamer._format_date_for_query("2024-01-15")

        assert result == "20240115"

    def test_invalid_format_fallback(self):
        """Test fallback pour format invalide."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        result = streamer._format_date_for_query("2024/01/15")

        # Fallback: retire les tirets et prend les 8 premiers caractères
        assert result == "2024/01/"

    def test_already_compact_format(self):
        """Test format déjà compact."""
        streamer = AMUEDataStreamer(Mock(), "http://api")

        result = streamer._format_date_for_query("20240115")

        assert result == "20240115"


class TestStreamData:
    """Tests pour stream_data."""

    def test_stream_single_page(self):
        """Test streaming avec une seule page."""
        mock_hook = Mock()
        streamer = AMUEDataStreamer(mock_hook, "http://api")

        # Mock de _fetch_page retournant une page unique
        rows = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        streamer._fetch_page = Mock(return_value=(rows, False))

        result = list(streamer.stream_data("TABLE", {"import_type": "full"}))

        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "A"}
        assert result[1] == {"id": 2, "name": "B"}
        streamer._fetch_page.assert_called_once()

    def test_stream_multiple_pages(self):
        """Test streaming avec plusieurs pages."""
        mock_hook = Mock()
        streamer = AMUEDataStreamer(mock_hook, "http://api")

        # Mock de _fetch_page retournant plusieurs pages
        page1 = ([{"id": 1}, {"id": 2}], True)
        page2 = ([{"id": 3}], False)
        streamer._fetch_page = Mock(side_effect=[page1, page2])

        result = list(streamer.stream_data("TABLE", {"import_type": "full"}))

        assert len(result) == 3
        assert streamer._fetch_page.call_count == 2

    def test_stream_empty_response(self):
        """Test streaming avec réponse vide."""
        mock_hook = Mock()
        streamer = AMUEDataStreamer(mock_hook, "http://api")

        streamer._fetch_page = Mock(return_value=([], False))

        result = list(streamer.stream_data("TABLE", {"import_type": "full"}))

        assert result == []

    def test_stream_yields_rows_individually(self):
        """Test que les lignes sont yieldées une par une."""
        mock_hook = Mock()
        streamer = AMUEDataStreamer(mock_hook, "http://api")

        rows = [{"id": i} for i in range(10)]
        streamer._fetch_page = Mock(return_value=(rows, False))

        gen = streamer.stream_data("TABLE", {"import_type": "full"})

        # Vérifie que c'est un générateur
        assert hasattr(gen, '__iter__')
        assert hasattr(gen, '__next__')

        # Consomme partiellement
        first = next(gen)
        assert first == {"id": 0}


class TestFetchPage:
    """Tests pour _fetch_page."""

    @patch('amue.operators.pipeline.data_streamer.get_retry_service')
    def test_fetch_page_success(self, mock_get_retry):
        """Test récupération de page réussie."""
        mock_hook = Mock()
        mock_hook.call_api.return_value = {
            "data": {
                "row": [{"id": 1}, {"id": 2}],
                "count": 2,
                "top": 99
            }
        }

        # Mock du retry service
        mock_service = Mock()
        mock_result = RetryResult(
            success=True,
            result=mock_hook.call_api.return_value,
            attempts=1
        )
        mock_service.execute_with_retry.return_value = mock_result
        mock_get_retry.return_value = mock_service

        streamer = AMUEDataStreamer(mock_hook, "http://api")
        params = {"nom": "TABLE", "f": "json", "skip": 0}

        rows, has_more = streamer._fetch_page(params, 1)

        assert len(rows) == 2
        assert has_more is False  # count <= skip + len(rows)

    @patch('amue.operators.pipeline.data_streamer.get_retry_service')
    def test_fetch_page_has_more(self, mock_get_retry):
        """Test détection de pages supplémentaires."""
        mock_hook = Mock()
        response = {
            "data": {
                "row": [{"id": i} for i in range(99)],  # Page complète
                "count": 200,  # Plus de données disponibles
                "top": 99
            }
        }

        mock_service = Mock()
        mock_result = RetryResult(success=True, result=response, attempts=1)
        mock_service.execute_with_retry.return_value = mock_result
        mock_get_retry.return_value = mock_service

        streamer = AMUEDataStreamer(mock_hook, "http://api")
        params = {"nom": "TABLE", "f": "json", "skip": 0}

        rows, has_more = streamer._fetch_page(params, 1)

        assert len(rows) == 99
        assert has_more is True

    @patch('amue.operators.pipeline.data_streamer.get_retry_service')
    def test_fetch_page_single_row_not_list(self, mock_get_retry):
        """Test conversion d'une ligne unique en liste."""
        mock_hook = Mock()
        response = {
            "data": {
                "row": {"id": 1},  # Pas une liste
                "count": 1,
                "top": 99
            }
        }

        mock_service = Mock()
        mock_result = RetryResult(success=True, result=response, attempts=1)
        mock_service.execute_with_retry.return_value = mock_result
        mock_get_retry.return_value = mock_service

        streamer = AMUEDataStreamer(mock_hook, "http://api")
        params = {"nom": "TABLE", "f": "json", "skip": 0}

        rows, has_more = streamer._fetch_page(params, 1)

        assert rows == [{"id": 1}]

    @patch('amue.operators.pipeline.data_streamer.get_retry_service')
    def test_fetch_page_empty_row(self, mock_get_retry):
        """Test réponse avec row vide."""
        mock_hook = Mock()
        response = {
            "data": {
                "row": None,
                "count": 0,
                "top": 99
            }
        }

        mock_service = Mock()
        mock_result = RetryResult(success=True, result=response, attempts=1)
        mock_service.execute_with_retry.return_value = mock_result
        mock_get_retry.return_value = mock_service

        streamer = AMUEDataStreamer(mock_hook, "http://api")
        params = {"nom": "TABLE", "f": "json", "skip": 0}

        rows, has_more = streamer._fetch_page(params, 1)

        assert rows == []
        assert has_more is False

    @patch('amue.operators.pipeline.data_streamer.get_retry_service')
    def test_fetch_page_failure_raises_exception(self, mock_get_retry):
        """Test que l'échec lève une exception."""
        mock_hook = Mock()

        mock_service = Mock()
        mock_result = RetryResult(
            success=False,
            error=Exception("API Error"),
            error_category=ErrorCategory.SERVER_ERROR,
            attempts=3
        )
        mock_service.execute_with_retry.return_value = mock_result
        mock_service.get_retry_info.return_value = {"recommendation": "Wait and retry"}
        mock_get_retry.return_value = mock_service

        streamer = AMUEDataStreamer(mock_hook, "http://api")
        params = {"nom": "TABLE", "f": "json", "skip": 0}

        with pytest.raises(AirflowException):
            streamer._fetch_page(params, 1)


class TestBuildErrorMessage:
    """Tests pour _build_error_message."""

    def test_client_error_message(self):
        """Test message pour erreur client."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        mock_result = Mock()
        mock_result.error_category = ErrorCategory.CLIENT_ERROR
        mock_result.error = "Bad Request"
        mock_result.attempts = 1

        msg = streamer._build_error_message(mock_result, Mock())

        assert "4xx" in msg
        assert "Pas de retry automatique" in msg

    def test_rate_limited_message(self):
        """Test message pour rate limit."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        mock_result = Mock()
        mock_result.error_category = ErrorCategory.RATE_LIMITED
        mock_result.attempts = 5
        mock_result.total_delay = 30.0

        msg = streamer._build_error_message(mock_result, Mock())

        assert "429" in msg
        assert "5 tentatives" in msg

    def test_server_error_message(self):
        """Test message pour erreur serveur."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        mock_result = Mock()
        mock_result.error_category = ErrorCategory.SERVER_ERROR
        mock_result.attempts = 3

        msg = streamer._build_error_message(mock_result, Mock())

        assert "5xx" in msg
        assert "AMUE est peut-etre indisponible" in msg

    def test_timeout_message(self):
        """Test message pour timeout."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        mock_result = Mock()
        mock_result.error_category = ErrorCategory.TIMEOUT
        mock_result.attempts = 2

        msg = streamer._build_error_message(mock_result, Mock())

        assert "Timeout" in msg
        assert "connectivite" in msg

    def test_unknown_error_message(self):
        """Test message pour erreur inconnue."""
        streamer = AMUEDataStreamer(Mock(), "http://api")
        mock_result = Mock()
        mock_result.error_category = ErrorCategory.UNKNOWN
        mock_result.attempts = 3
        mock_result.error = "Unknown error"

        msg = streamer._build_error_message(mock_result, Mock())

        assert "Impossible de recuperer" in msg
        assert "Unknown error" in msg
