"""Tests pour sql_file_loader (lecture de .sql + vérification d'intégrité)."""
import pytest

from common.infrastructure.database.sql_file_loader import (
    read_sql_file,
    verify_sql_file_integrity,
)


class TestVerifySqlFileIntegrity:
    def test_no_manifest_does_not_raise(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        verify_sql_file_integrity(sql_file)  # pas de manifest -> OK silencieux

    def test_file_absent_from_manifest_does_not_raise(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        (tmp_path / ".manifest.sha256").write_text(
            "deadbeef  other.sql\n", encoding="utf-8"
        )
        verify_sql_file_integrity(sql_file)

    def test_matching_hash_does_not_raise(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        import hashlib
        digest = hashlib.sha256(sql_file.read_bytes()).hexdigest()
        (tmp_path / ".manifest.sha256").write_text(
            f"{digest}  a.sql\n", encoding="utf-8"
        )
        verify_sql_file_integrity(sql_file)

    def test_mismatched_hash_raises(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        (tmp_path / ".manifest.sha256").write_text(
            "deadbeef  a.sql\n", encoding="utf-8"
        )
        with pytest.raises(ValueError, match="Intégrité fichier SQL compromise"):
            verify_sql_file_integrity(sql_file)


class TestReadSqlFile:
    def test_reads_content(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        assert read_sql_file(sql_file) == "SELECT 1;"

    def test_verify_integrity_false_skips_check(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        (tmp_path / ".manifest.sha256").write_text(
            "deadbeef  a.sql\n", encoding="utf-8"
        )
        # Avec verify_integrity=False, pas d'exception malgré un manifest invalide.
        assert read_sql_file(sql_file, verify_integrity=False) == "SELECT 1;"

    def test_verify_integrity_true_raises_on_mismatch(self, tmp_path):
        sql_file = tmp_path / "a.sql"
        sql_file.write_text("SELECT 1;", encoding="utf-8")
        (tmp_path / ".manifest.sha256").write_text(
            "deadbeef  a.sql\n", encoding="utf-8"
        )
        with pytest.raises(ValueError):
            read_sql_file(sql_file)
