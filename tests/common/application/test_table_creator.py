"""Tests pour table_creator.build_meta_column_defs."""
from common.application.table_creator import build_meta_column_defs


class TestBuildMetaColumnDefs:
    def test_includes_source_with_default(self):
        defs = build_meta_column_defs('sifac_plus')
        assert any("_source VARCHAR(50) DEFAULT 'sifac_plus'" == d for d in defs)

    def test_includes_imported_at(self):
        defs = build_meta_column_defs('ecc')
        assert "_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" in defs

    def test_returns_two_fragments_in_order(self):
        defs = build_meta_column_defs('ecc')
        assert len(defs) == 2
        assert defs[0].startswith('_source')
        assert defs[1].startswith('_imported_at')
