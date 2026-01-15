# amue/notifications/notifiers/success_notifier.py
"""Notifier pour les succès"""
from datetime import datetime
from typing import Dict, Any, List
from amue.notifications.notifiers.base import BaseNotifier
from amue.notifications.templates.base import BaseTemplate
from amue.notifications.templates.success import SuccessTemplate


class SuccessNotifier(BaseNotifier):
    """
    Notifier pour les imports réussis

    Usage:
        notifier = SuccessNotifier()
        notifier.notify({
            'dag_id': 'my_dag',
            'tables_imported': [
                {'table_name': 'TABLE1', 'rows_inserted': 1000, 'status': 'success'},
                {'table_name': 'TABLE2', 'rows_inserted': 500, 'status': 'success'}
            ],
            'duration': '5m 30s'
        })
    """

    @property
    def template(self) -> BaseTemplate:
        return SuccessTemplate()

    def build_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construit le contexte pour le template de succès

        Args:
            data: Données de l'import réussi

        Returns:
            Contexte formaté
        """
        dag_id = data.get('dag_id', 'amue_multi_table_import')
        execution_date = data.get('execution_date', datetime.now().isoformat())
        duration = data.get('duration', 'N/A')
        tables_imported = data.get('tables_imported', [])

        # Calcule le total de lignes
        total_rows = sum(
            t.get('rows_inserted', t.get('rows', 0))
            for t in tables_imported
        )

        return {
            'title': 'Import AMUE Reussi',
            'subtitle': execution_date,
            'dag_id': dag_id,
            'execution_date': execution_date,
            'duration': duration,
            'tables_imported': tables_imported,
            'total_rows': total_rows,
            'status': 'success'
        }

    def build_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email de succès"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        tables_count = len(context.get('tables_imported', []))
        total_rows = context.get('total_rows', 0)
        return f"[SUCCES] Import AMUE - {tables_count} table(s) - {total_rows:,} lignes - {date_str}"
