# tests/operators/pipeline/test_duplicate_detector.py
"""
Tests unitaires pour DuplicateDetector.
"""
import pytest
from unittest.mock import patch, MagicMock

from amue.operators.pipeline.duplicate_detector import DuplicateDetector


class TestDetectDuplicatesInBatch:
    """Tests pour detect_duplicates_in_batch."""

    def test_no_duplicates_returns_empty(self):
        """Test retourne vide si pas de doublons."""
        detector = DuplicateDetector()
        batch = [
            (1, "A", "value1"),
            (2, "B", "value2"),
            (3, "C", "value3")
        ]
        columns = ["id", "name", "value"]
        primary_keys = ["id"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        assert result == {}

    def test_detects_single_duplicate_group(self):
        """Test détection d'un groupe de doublons."""
        detector = DuplicateDetector()
        batch = [
            (1, "A", "value1"),
            (1, "B", "value2"),  # Doublon sur id=1
            (3, "C", "value3")
        ]
        columns = ["id", "name", "value"]
        primary_keys = ["id"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        assert len(result) == 1
        key = "id=1"
        assert key in result
        assert len(result[key]) == 2

    def test_detects_multiple_duplicate_groups(self):
        """Test détection de plusieurs groupes de doublons."""
        detector = DuplicateDetector()
        batch = [
            (1, "A"),
            (1, "B"),  # Doublon sur id=1
            (2, "C"),
            (2, "D"),  # Doublon sur id=2
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        assert len(result) == 2

    def test_composite_primary_key(self):
        """Test avec clé primaire composite."""
        detector = DuplicateDetector()
        batch = [
            ("A", 1, "val1"),
            ("A", 1, "val2"),  # Doublon sur (code, num)
            ("A", 2, "val3"),
            ("B", 1, "val4"),
        ]
        columns = ["code", "num", "value"]
        primary_keys = ["code", "num"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        assert len(result) == 1
        # La clé devrait être "code=A|num=1"
        assert any("A" in k and "1" in k for k in result.keys())

    def test_adds_batch_index_to_duplicates(self):
        """Test que l'index de batch est ajouté."""
        detector = DuplicateDetector()
        batch = [
            (1, "A"),
            (1, "B"),
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        key = list(result.keys())[0]
        assert result[key][0]["_batch_index"] == 0
        assert result[key][1]["_batch_index"] == 1

    def test_handles_none_values_in_pk(self):
        """Test gestion des valeurs None dans PK."""
        detector = DuplicateDetector()
        batch = [
            (None, "A"),
            (None, "B"),  # Doublon sur pk null
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        # Les None sont convertis en chaîne vide
        assert len(result) == 1

    def test_empty_batch_returns_empty(self):
        """Test batch vide retourne vide."""
        detector = DuplicateDetector()

        result = detector.detect_duplicates_in_batch([], ["id"], ["id"])

        assert result == {}

    def test_invalid_pk_column_returns_empty(self):
        """Test colonne PK invalide retourne vide."""
        detector = DuplicateDetector()
        batch = [(1, "A")]
        columns = ["id", "name"]
        primary_keys = ["nonexistent"]

        result = detector.detect_duplicates_in_batch(batch, columns, primary_keys)

        assert result == {}


class TestFindDuplicatesForPk:
    """Tests pour find_duplicates_for_pk."""

    def test_finds_all_matching_rows(self):
        """Test trouve toutes les lignes correspondantes."""
        detector = DuplicateDetector()
        batch = [
            (1, "A"),
            (2, "B"),
            (1, "C"),
            (3, "D"),
            (1, "E"),
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]
        pk_values = {"id": "1"}

        result = detector.find_duplicates_for_pk(batch, columns, primary_keys, pk_values)

        assert len(result) == 3
        assert all(r["id"] == 1 for r in result)

    def test_returns_empty_if_no_match(self):
        """Test retourne vide si pas de correspondance."""
        detector = DuplicateDetector()
        batch = [
            (1, "A"),
            (2, "B"),
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]
        pk_values = {"id": "999"}

        result = detector.find_duplicates_for_pk(batch, columns, primary_keys, pk_values)

        assert result == []

    def test_adds_batch_index(self):
        """Test ajoute l'index de batch."""
        detector = DuplicateDetector()
        batch = [
            (1, "A"),
            (1, "B"),
        ]
        columns = ["id", "name"]
        primary_keys = ["id"]
        pk_values = {"id": "1"}

        result = detector.find_duplicates_for_pk(batch, columns, primary_keys, pk_values)

        assert result[0]["_batch_index"] == 0
        assert result[1]["_batch_index"] == 1


class TestExtractPkFromError:
    """Tests pour extract_pk_from_error."""

    def test_extracts_single_column_pk(self):
        """Test extraction PK simple."""
        detector = DuplicateDetector()
        error_msg = "DETAIL: La cle (id)=(42) existe deja."
        primary_keys = ["id"]

        result = detector.extract_pk_from_error(error_msg, primary_keys)

        assert result == {"id": "42"}

    def test_extracts_composite_pk(self):
        """Test extraction PK composite."""
        detector = DuplicateDetector()
        error_msg = "DETAIL: La cle (code, num)=(ABC, 123) existe deja."
        primary_keys = ["code", "num"]

        result = detector.extract_pk_from_error(error_msg, primary_keys)

        assert result == {"code": "ABC", "num": "123"}

    def test_handles_quoted_values(self):
        """Test valeurs avec guillemets."""
        detector = DuplicateDetector()
        error_msg = "DETAIL: Key (id)=('value with, comma') already exists."
        primary_keys = ["id"]

        result = detector.extract_pk_from_error(error_msg, primary_keys)

        assert result is not None
        assert "id" in result

    def test_returns_none_on_parse_failure(self):
        """Test retourne None si parsing échoue."""
        detector = DuplicateDetector()
        error_msg = "Some random error message without key info"
        primary_keys = ["id"]

        result = detector.extract_pk_from_error(error_msg, primary_keys)

        assert result is None

    def test_handles_english_error_message(self):
        """Test message d'erreur en anglais."""
        detector = DuplicateDetector()
        error_msg = "duplicate key value violates unique constraint Key (code)=(TEST) already exists."
        primary_keys = ["code"]

        result = detector.extract_pk_from_error(error_msg, primary_keys)

        assert result == {"code": "TEST"}


class TestParsePkValues:
    """Tests pour _parse_pk_values."""

    def test_simple_values(self):
        """Test valeurs simples."""
        detector = DuplicateDetector()

        result = detector._parse_pk_values("val1, val2, val3")

        assert result == ["val1", "val2", "val3"]

    def test_value_with_comma_in_quotes(self):
        """Test valeur avec virgule entre guillemets."""
        detector = DuplicateDetector()

        result = detector._parse_pk_values("'val1, with comma', val2")

        assert len(result) == 2
        assert "val1, with comma" in result[0]

    def test_empty_string(self):
        """Test chaîne vide."""
        detector = DuplicateDetector()

        result = detector._parse_pk_values("")

        assert result == []


class TestGetPkIndices:
    """Tests pour _get_pk_indices."""

    def test_finds_single_pk_index(self):
        """Test trouve l'index d'une PK simple."""
        detector = DuplicateDetector()
        columns = ["id", "name", "value"]
        primary_keys = ["id"]

        result = detector._get_pk_indices(columns, primary_keys)

        assert result == [("id", 0)]

    def test_finds_multiple_pk_indices(self):
        """Test trouve les indices de PK composite."""
        detector = DuplicateDetector()
        columns = ["id", "code", "name"]
        primary_keys = ["id", "code"]

        result = detector._get_pk_indices(columns, primary_keys)

        assert ("id", 0) in result
        assert ("code", 1) in result

    def test_handles_case_insensitivity(self):
        """Test insensibilité à la casse."""
        detector = DuplicateDetector()
        columns = ["id", "name"]
        primary_keys = ["ID"]  # Majuscules

        result = detector._get_pk_indices(columns, primary_keys)

        assert result == [("id", 0)]

    def test_missing_pk_column_ignored(self):
        """Test colonne PK manquante ignorée."""
        detector = DuplicateDetector()
        columns = ["id", "name"]
        primary_keys = ["id", "nonexistent"]

        result = detector._get_pk_indices(columns, primary_keys)

        assert len(result) == 1
        assert result == [("id", 0)]


class TestBuildPkKey:
    """Tests pour _build_pk_key."""

    def test_builds_single_pk_key(self):
        """Test construction clé PK simple."""
        detector = DuplicateDetector()
        record = (42, "test")
        pk_indices = [("id", 0)]

        key, values = detector._build_pk_key(record, pk_indices)

        assert key == "id=42"
        assert values == {"id": "42"}

    def test_builds_composite_pk_key(self):
        """Test construction clé PK composite."""
        detector = DuplicateDetector()
        record = ("ABC", 123, "test")
        pk_indices = [("code", 0), ("num", 1)]

        key, values = detector._build_pk_key(record, pk_indices)

        assert key == "code=ABC|num=123"
        assert values == {"code": "ABC", "num": "123"}

    def test_handles_none_value(self):
        """Test gestion valeur None."""
        detector = DuplicateDetector()
        record = (None, "test")
        pk_indices = [("id", 0)]

        key, values = detector._build_pk_key(record, pk_indices)

        assert key == "id="
        assert values == {"id": ""}

    def test_strips_whitespace(self):
        """Test supprime les espaces."""
        detector = DuplicateDetector()
        record = ("  ABC  ", "test")
        pk_indices = [("code", 0)]

        key, values = detector._build_pk_key(record, pk_indices)

        assert key == "code=ABC"
        assert values == {"code": "ABC"}


class TestMatchesPk:
    """Tests pour _matches_pk."""

    def test_matches_single_pk(self):
        """Test correspondance PK simple."""
        detector = DuplicateDetector()
        record = (42, "test")
        pk_indices = [("id", 0)]
        pk_values = {"id": "42"}

        result = detector._matches_pk(record, pk_indices, pk_values)

        assert result is True

    def test_no_match_different_value(self):
        """Test pas de correspondance si valeur différente."""
        detector = DuplicateDetector()
        record = (42, "test")
        pk_indices = [("id", 0)]
        pk_values = {"id": "99"}

        result = detector._matches_pk(record, pk_indices, pk_values)

        assert result is False

    def test_matches_composite_pk(self):
        """Test correspondance PK composite."""
        detector = DuplicateDetector()
        record = ("ABC", 123, "test")
        pk_indices = [("code", 0), ("num", 1)]
        pk_values = {"code": "ABC", "num": "123"}

        result = detector._matches_pk(record, pk_indices, pk_values)

        assert result is True

    def test_partial_match_returns_false(self):
        """Test correspondance partielle retourne False."""
        detector = DuplicateDetector()
        record = ("ABC", 123, "test")
        pk_indices = [("code", 0), ("num", 1)]
        pk_values = {"code": "ABC", "num": "999"}  # num différent

        result = detector._matches_pk(record, pk_indices, pk_values)

        assert result is False


class TestLogMethods:
    """Tests pour les méthodes de logging."""

    @patch('amue.operators.pipeline.duplicate_detector.logger')
    def test_log_batch_duplicates_logs_summary(self, mock_logger):
        """Test que log_batch_duplicates log un résumé."""
        detector = DuplicateDetector()
        duplicates_groups = {
            "id=1": [
                {"id": 1, "name": "A", "_batch_index": 0, "_pk_values": {"id": "1"}},
                {"id": 1, "name": "B", "_batch_index": 1, "_pk_values": {"id": "1"}}
            ]
        }

        detector.log_batch_duplicates("test_table", ["id", "name"], ["id"], duplicates_groups)

        # Vérifie qu'il y a eu des appels à logger.error
        assert mock_logger.error.called

    @patch('amue.operators.pipeline.duplicate_detector.logger')
    def test_log_api_duplicates_logs_details(self, mock_logger):
        """Test que log_api_duplicates log les détails."""
        detector = DuplicateDetector()
        duplicates = [
            {"id": 1, "name": "A", "_batch_index": 0},
            {"id": 1, "name": "B", "_batch_index": 1}
        ]
        pk_values = {"id": "1"}

        detector.log_api_duplicates("test_table", ["id", "name"], ["id"], duplicates, pk_values)

        assert mock_logger.error.called

    @patch('amue.operators.pipeline.duplicate_detector.logger')
    def test_log_conflict_details_shows_comparison(self, mock_logger):
        """Test que log_conflict_details affiche une comparaison."""
        detector = DuplicateDetector()
        existing_row = {"id": 1, "name": "Existing"}
        new_row = {"id": 1, "name": "New"}
        pk_values = {"id": "1"}

        detector.log_conflict_details(
            "test_table", ["id", "name"], ["id"],
            existing_row, new_row, pk_values
        )

        assert mock_logger.error.called

    @patch('amue.operators.pipeline.duplicate_detector.logger')
    def test_log_conflict_details_handles_none_rows(self, mock_logger):
        """Test log_conflict_details avec lignes None."""
        detector = DuplicateDetector()
        pk_values = {"id": "1"}

        # Ne devrait pas lever d'exception
        detector.log_conflict_details(
            "test_table", ["id", "name"], ["id"],
            None, None, pk_values
        )

        assert mock_logger.error.called
