# ecc/notifications/ecc_templates.py
"""Templates HTML pour les notifications ECC."""
from typing import Any, Dict, List

from amue.notifications.templates import NotificationTemplates


class ECCNotificationTemplates(NotificationTemplates):
    """
    Templates HTML pour les notifications ECC.

    Surcharge NotificationTemplates pour :
    - Titre "Import ECC" (succes/erreur)
    - Badge ECC orange dans le rapport de succes
    - Colonne "Ignorees" (rows_skipped) dans le tableau des tables
    """

    @classmethod
    def render_success(cls, context: Dict[str, Any]) -> str:
        """Rapport de succes ECC avec badge orange et colonne Ignorees."""
        title = context.get('title', 'Import ECC Réussi')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'ecc_multi_table_import')
        execution_date = context.get('execution_date', '')
        duration = context.get('duration', 'N/A')
        tables_imported = context.get('tables_imported', [])
        total_rows = context.get('total_rows', 0)
        total_fetched = context.get('total_fetched', total_rows)
        total_updated = context.get('total_updated', 0)
        total_skipped = context.get('total_skipped', 0)

        tables_html = cls._render_tables_list(tables_imported)

        content = f"""
        <div style="background: #e8f5e9; border-left: 4px solid #4CAF50; padding: 20px; border-radius: 4px;">
            <h2 style="color: #2e7d32; margin-top: 0; font-size: 18px;">
                Import ECC Terminé avec Succès
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

                <div class="info-label">Lignes source :</div>
                <div class="info-value"><strong>{total_fetched:,}</strong> ligne(s) récupérée(s)</div>

                <div class="info-label">Lignes DB :</div>
                <div class="info-value"><strong>{total_rows:,}</strong> ligne(s) insérée(s)</div>

                <div class="info-label">Mises à jour :</div>
                <div class="info-value"><strong>{total_updated:,}</strong> ligne(s) mise(s) à jour</div>

                <div class="info-label">Ignorées :</div>
                <div class="info-value"><strong>{total_skipped:,}</strong> ligne(s) protégée(s) sifac_plus</div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge" style="background: #fff3e0; color: #ef6c00;">ECC</span>
                    <span class="badge badge-success" style="margin-left: 6px;">succès</span>
                </div>
            </div>
        </div>

        {tables_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_SUCCESS)
        footer = cls._render_footer()

        return cls._wrap_html(header, content, footer)

    @classmethod
    def render_error(cls, context: Dict[str, Any]) -> str:
        """Notification d'erreur ECC."""
        # Surcharge uniquement le titre par défaut
        if 'title' not in context:
            context = dict(context, title="Erreur Import ECC")
        return super().render_error(context)

    @classmethod
    def _render_tables_list(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu du tableau des tables avec colonne Ignorees (rows_skipped)."""
        if not tables:
            return ''

        rows_html = ''
        for table in tables:
            name = table.get('table_name', table.get('name', 'unknown'))
            rows_fetched = table.get('rows_fetched', 0)
            rows_inserted = table.get('rows_inserted', table.get('rows', 0))
            rows_updated = table.get('rows_updated', 0)
            rows_skipped = table.get('rows_skipped', 0)
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
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_skipped:,}
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
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e0e0e0;">Table</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Récupérées</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Insérées</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">MAJ</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Ignorées</th>
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
