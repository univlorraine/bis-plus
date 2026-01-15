# amue/notifications/templates/success.py
"""Template pour les notifications de succès"""
from typing import Dict, Any, List
from amue.notifications.templates.base import BaseTemplate


class SuccessTemplate(BaseTemplate):
    """Template pour les emails de succès"""

    @property
    def header_color(self) -> str:
        return "linear-gradient(135deg, #4CAF50 0%, #388E3C 100%)"

    def render_content(self, context: Dict[str, Any]) -> str:
        """
        Rendu du contenu de succès

        Context attendu:
            - dag_id: ID du DAG
            - execution_date: Date d'exécution
            - duration: Durée d'exécution
            - tables_imported: Liste des tables importées
            - total_rows: Nombre total de lignes
            - summary: Résumé des opérations
        """
        dag_id = context.get('dag_id', 'unknown')
        execution_date = context.get('execution_date', '')
        duration = context.get('duration', 'N/A')
        tables_imported = context.get('tables_imported', [])
        total_rows = context.get('total_rows', 0)

        # Rendu de la liste des tables
        tables_html = self._render_tables_list(tables_imported)

        return f"""
        <div style="background: #e8f5e9; border-left: 4px solid #4CAF50; padding: 20px; border-radius: 4px;">
            <h2 style="color: #2e7d32; margin-top: 0; font-size: 18px;">
                Import Termine avec Succes
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG:</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Date:</div>
                <div class="info-value">{execution_date}</div>

                <div class="info-label">Duree:</div>
                <div class="info-value">{duration}</div>

                <div class="info-label">Tables:</div>
                <div class="info-value"><strong>{len(tables_imported)}</strong> table(s)</div>

                <div class="info-label">Lignes:</div>
                <div class="info-value"><strong>{total_rows:,}</strong> ligne(s) importee(s)</div>

                <div class="info-label">Statut:</div>
                <div class="info-value">
                    <span class="badge badge-success">succes</span>
                </div>
            </div>
        </div>

        {tables_html}
        """

    def _render_tables_list(self, tables: List[Dict[str, Any]]) -> str:
        """Rendu de la liste des tables importées"""
        if not tables:
            return ''

        rows_html = ''
        for table in tables:
            name = table.get('table_name', table.get('name', 'unknown'))
            rows = table.get('rows_inserted', table.get('rows', 0))
            import_type = table.get('import_type', 'full')
            status = table.get('status', 'success')

            badge_class = 'badge-success' if status == 'success' else 'badge-error'

            rows_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;">
                    <strong>{name}</strong>
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    <code>{import_type}</code>
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    <span class="badge {badge_class}">{status}</span>
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #333; font-size: 16px; margin-bottom: 15px;">
                Detail des tables importees
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e0e0e0;">Table</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Lignes</th>
                        <th style="padding: 12px; text-align: center; border-bottom: 2px solid #e0e0e0;">Type</th>
                        <th style="padding: 12px; text-align: center; border-bottom: 2px solid #e0e0e0;">Statut</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """
