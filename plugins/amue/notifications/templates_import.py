# amue/notifications/templates_import.py
"""Template d'email pour les rapports d'import (AMUE, ECC)."""
from typing import Any, Dict, List

from amue.notifications.templates_base import BaseTemplates


class ImportTemplates(BaseTemplates):
    """Template HTML pour les notifications de succès d'import."""

    @classmethod
    def render_success(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour un rapport d'import réussi.

        Contexte attendu :
            title           : titre de l'email
            subtitle        : sous-titre (date d'exécution)
            dag_id          : ID du DAG
            execution_date  : date d'exécution ISO
            duration        : durée formatée (ex. '12m 30s')
            tables_imported : liste de dicts par table
            total_rows      : lignes insérées (total)
            total_fetched   : lignes récupérées depuis l'API
            total_updated   : lignes mises à jour
            extra_message   : message complémentaire (optionnel)
        """
        title = context.get('title', 'Import Réussi')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        execution_date = context.get('execution_date', '')
        duration = context.get('duration', 'N/A')
        tables_imported = context.get('tables_imported', [])
        total_rows = context.get('total_rows', 0)
        total_fetched = context.get('total_fetched', total_rows)
        total_updated = context.get('total_updated', 0)
        extra_message = context.get('extra_message', '')

        tables_html = cls._render_tables_list(tables_imported)

        extra_message_html = ''
        if extra_message:
            extra_message_html = f"""
        <div style="background: #e3f2fd; border-left: 4px solid #2196F3; padding: 15px;
                    border-radius: 4px; margin-top: 15px;">
            <pre style="margin: 0; white-space: pre-wrap; font-family: inherit;
                        font-size: 14px;">{cls.escape_html(extra_message)}</pre>
        </div>"""

        content = f"""
        <div style="background: #e8f5e9; border-left: 4px solid #4CAF50;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #2e7d32; margin-top: 0; font-size: 18px;">
                Import Terminé avec Succès
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Date :</div>
                <div class="info-value">{execution_date}</div>

                <div class="info-label">Durée :</div>
                <div class="info-value"><strong>{duration}</strong></div>

                <div class="info-label">Tables :</div>
                <div class="info-value"><strong>{len(tables_imported)}</strong> table(s)</div>

                <div class="info-label">Lignes API :</div>
                <div class="info-value">
                    <strong>{total_fetched:,}</strong> ligne(s) récupérée(s)
                </div>

                <div class="info-label">Lignes DB :</div>
                <div class="info-value">
                    <strong>{total_rows:,}</strong> ligne(s) insérée(s)
                </div>

                <div class="info-label">Mises à jour :</div>
                <div class="info-value">
                    <strong>{total_updated:,}</strong> ligne(s) mise(s) à jour
                </div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-success">succès</span>
                </div>
            </div>
        </div>
        {extra_message_html}
        {tables_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_SUCCESS)
        footer = cls._render_footer()
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_tables_list(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu du tableau détaillé des tables importées."""
        if not tables:
            return ''

        rows_html = ''
        for table in tables:
            name = table.get('table_name', 'unknown')
            rows_fetched = table.get('rows_fetched', 0)
            rows_inserted = table.get('rows_inserted', table.get('rows', 0))
            rows_updated = table.get('rows_updated', 0)
            status = table.get('status', 'success')
            import_type = table.get('import_type', 'full')

            badge_class = 'badge-success' if status == 'success' else 'badge-error'
            rows_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;">
                    <strong>{name}</strong>
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_fetched:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_inserted:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_updated:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    {import_type}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    <span class="badge {badge_class}">{status}</span>
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #333; font-size: 16px; margin-bottom: 15px;">
                Détail des tables importées
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="padding: 12px; text-align: left;
                                   border-bottom: 2px solid #e0e0e0;">Table</th>
                        <th style="padding: 12px; text-align: right;
                                   border-bottom: 2px solid #e0e0e0;">Récupérées</th>
                        <th style="padding: 12px; text-align: right;
                                   border-bottom: 2px solid #e0e0e0;">Insérées</th>
                        <th style="padding: 12px; text-align: right;
                                   border-bottom: 2px solid #e0e0e0;">MAJ</th>
                        <th style="padding: 12px; text-align: center;
                                   border-bottom: 2px solid #e0e0e0;">Type</th>
                        <th style="padding: 12px; text-align: center;
                                   border-bottom: 2px solid #e0e0e0;">Statut</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """
