"""Tests de fumée pour ECCNotificationTemplates (rendu HTML après extraction de build_info_grid)."""
from ecc.infrastructure.notifications.ecc_templates import ECCNotificationTemplates


class TestRenderSuccess:
    def test_contains_dag_id_and_counts(self):
        html = ECCNotificationTemplates.render_success({
            'dag_id': 'ecc_multi_table_import',
            'execution_date': '2026-01-01',
            'duration': '5s',
            'tables_imported': [{'table_name': 'CSKS', 'rows_inserted': 10}],
            'total_rows': 10,
            'total_fetched': 10,
            'total_updated': 0,
            'total_skipped': 2,
        })
        assert 'ecc_multi_table_import' in html
        assert '2026-01-01' in html
        assert '10' in html
        assert 'CSKS' in html
        assert 'info-label' in html and 'info-value' in html

    def test_empty_tables_does_not_raise(self):
        html = ECCNotificationTemplates.render_success({'tables_imported': []})
        assert 'info-grid' in html


class TestRenderError:
    def test_contains_error_details(self):
        html = ECCNotificationTemplates.render_error({
            'dag_id': 'ecc_multi_table_import',
            'task_id': 'import_data',
            'error_type': 'ValueError',
            'error_message': 'boom',
        })
        assert 'ecc_multi_table_import' in html
        assert 'import_data' in html
        assert 'ValueError' in html
        assert 'boom' in html
