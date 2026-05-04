# amue/notifications/templates_sync.py
"""Template d'email pour les rapports de synchronisation blue/green."""
from typing import Any, Dict, List

from common.notifications.base_templates import BaseTemplates


class SyncTemplates(BaseTemplates):
    """Template HTML pour les notifications de synchronisation blue/green."""

    @classmethod
    def render_sync_success(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour un rapport de synchronisation réussie.

        Contexte attendu :
            title          : titre de l'email
            subtitle       : date d'exécution
            dag_id         : ID du DAG
            source         : schéma source (ex. 'splus_blue')
            target         : schéma cible (ex. 'splus_green')
            tables_synced  : nombre de tables synchronisées avec succès
            tables_failed  : nombre de tables en échec (0 si succès total)
            tables_detail  : liste de dicts {table_name, rows_inserted, status}
        """
        title = context.get('title', 'Synchronisation Réussie')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        source = context.get('source', '?')
        target = context.get('target', '?')
        tables_synced = context.get('tables_synced', 0)
        tables_failed = context.get('tables_failed', 0)
        tables_detail = context.get('tables_detail', [])

        total_rows = sum(t.get('rows_inserted', 0) for t in tables_detail)

        status_badge = (
            '<span class="badge badge-warning">partiel</span>'
            if tables_failed > 0
            else '<span class="badge badge-success">succès</span>'
        )

        tables_html = cls._render_sync_tables(tables_detail)

        content = f"""
        <div style="background: #e3f2fd; border-left: 4px solid #2196F3;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #1565c0; margin-top: 0; font-size: 18px;">
                Synchronisation Blue/Green Terminée
            </h2>

            <!-- Schémas source → cible -->
            <div style="display: flex; align-items: center; gap: 16px; margin: 20px 0;
                        background: #fff; padding: 16px; border-radius: 6px;
                        border: 1px solid #bbdefb;">
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase;
                                letter-spacing: 1px; margin-bottom: 4px;">Source</div>
                    <code style="font-size: 15px; background: #e3f2fd;
                                 padding: 6px 12px;">{source}</code>
                </div>
                <div style="font-size: 28px; color: #1565c0;">→</div>
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase;
                                letter-spacing: 1px; margin-bottom: 4px;">Cible</div>
                    <code style="font-size: 15px; background: #e3f2fd;
                                 padding: 6px 12px;">{target}</code>
                </div>
            </div>

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tables copiées :</div>
                <div class="info-value"><strong>{tables_synced}</strong> table(s)</div>

                <div class="info-label">Tables en échec :</div>
                <div class="info-value"><strong>{tables_failed}</strong> table(s)</div>

                <div class="info-label">Lignes copiées :</div>
                <div class="info-value"><strong>{total_rows:,}</strong> ligne(s)</div>

                <div class="info-label">Statut :</div>
                <div class="info-value">{status_badge}</div>
            </div>
        </div>
        {tables_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_SYNC)
        footer = cls._render_footer()
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_sync_tables(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu du tableau des tables synchronisées."""
        if not tables:
            return ''

        rows_html = ''
        for table in tables:
            name = table.get('table_name', 'unknown')
            rows = table.get('rows_inserted', table.get('rows_fetched', 0))
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
                    <span class="badge {badge_class}">{status}</span>
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #333; font-size: 16px; margin-bottom: 15px;">
                Détail des tables synchronisées
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="padding: 12px; text-align: left;
                                   border-bottom: 2px solid #e0e0e0;">Table</th>
                        <th style="padding: 12px; text-align: right;
                                   border-bottom: 2px solid #e0e0e0;">Lignes copiées</th>
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
