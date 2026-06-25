"""Tests pour BaseTemplates.build_info_grid."""
from common.infrastructure.notifications.base_templates import BaseTemplates


class TestBuildInfoGrid:
    def test_renders_label_value_pairs(self):
        html = BaseTemplates.build_info_grid([('DAG :', 'amue_import')])
        assert '<div class="info-label">DAG :</div>' in html
        assert '<div class="info-value">amue_import</div>' in html

    def test_renders_multiple_pairs_in_order(self):
        html = BaseTemplates.build_info_grid([
            ('A :', '1'),
            ('B :', '2'),
        ])
        assert html.index('A :') < html.index('B :')

    def test_value_not_escaped_allows_raw_html(self):
        html = BaseTemplates.build_info_grid([('Statut :', '<span class="badge">ok</span>')])
        assert '<span class="badge">ok</span>' in html

    def test_empty_list_returns_empty_string(self):
        assert BaseTemplates.build_info_grid([]) == ''
