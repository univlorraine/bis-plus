# amue/notifications/templates_setup.py
"""Template d'email pour les alertes de setup des tables."""
from typing import Any, Dict, List

from amue.notifications.templates_base import BaseTemplates


class SetupTemplates(BaseTemplates):
    """Template HTML pour les notifications d'anomalie de setup."""

    @classmethod
    def render_setup_error(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour une alerte de setup incomplet.

        Contexte attendu :
            title          : titre de l'email
            subtitle       : date
            dag_id         : ID du DAG
            tables_blocked : liste de dicts pour les tables bloquées
                             {table_name, fp_api_changed, fp_ul_changed,
                              columns_count, ul_diff}
            tables_error   : liste de dicts pour les tables en erreur
                             {table_name, error}
        """
        title = context.get('title', 'Anomalie Setup Tables')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        tables_blocked = context.get('tables_blocked', [])
        tables_error = context.get('tables_error', [])

        blocked_html = cls._render_blocked_tables(tables_blocked) if tables_blocked else ''
        error_html = cls._render_error_tables(tables_error) if tables_error else ''

        nb_blocked = len(tables_blocked)
        nb_error = len(tables_error)

        summary_parts = []
        if nb_blocked:
            summary_parts.append(
                f"<strong>{nb_blocked}</strong> table(s) bloquée(s) "
                "(changement de structure détecté)"
            )
        if nb_error:
            summary_parts.append(f"<strong>{nb_error}</strong> table(s) en erreur")

        summary_html = ' — '.join(summary_parts)

        content = f"""
        <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #e65100; margin-top: 0; font-size: 18px;">
                Setup Incomplet — Action Requise
            </h2>
            <p style="margin: 8px 0 0; color: #555;">
                {summary_html}
            </p>
            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-warning">incomplet</span>
                </div>
            </div>
        </div>

        {blocked_html}
        {error_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_WARNING)
        footer = cls._render_footer(
            dag_id,
            show_actions=True,
            actions=(
                "- Vérifiez les changements de structure dans l'API AMUE<br>"
                "- Mettez à jour la configuration locale si nécessaire<br>"
                "- Relancez <code>amue_table_setup</code> après correction"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_blocked_tables(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu du tableau des tables bloquées avec info fingerprint."""
        rows_html = ''
        for t in tables:
            name = t.get('table_name', 'unknown')
            fp_api = t.get('fp_api_changed')
            fp_ul = t.get('fp_ul_changed')
            cols = t.get('columns_count')
            ul_diff = t.get('ul_diff', '')

            # Cellule FP API
            if fp_api is True:
                fp_api_html = '<span class="badge badge-error">modifié</span>'
            elif fp_api is False:
                fp_api_html = '<span class="badge badge-success">inchangé</span>'
            else:
                fp_api_html = '<span style="color:#999">N/A</span>'

            # Cellule FP UL
            if fp_ul is True:
                fp_ul_html = '<span class="badge badge-error">modifié</span>'
            elif fp_ul is False:
                fp_ul_html = '<span class="badge badge-success">inchangé</span>'
            else:
                fp_ul_html = '<span style="color:#999">N/A</span>'

            # Cause probable
            if fp_api is True and fp_ul is True:
                cause = "Colonnes ajoutées / supprimées (API + config locale)"
            elif fp_api is True:
                cause = "Types ou colonnes côté API uniquement"
            elif fp_ul is True:
                cause = "Clés primaires UL ou types PG modifiés (config locale)"
            else:
                cause = t.get('error', 'Structure modifiée')

            # Diff colonnes (optionnel)
            diff_html = ''
            if ul_diff:
                escaped_diff = cls.escape_html(ul_diff)
                diff_html = f"""
                <div style="margin-top: 8px; background: #f8f9fa; padding: 8px;
                            border-radius: 3px; font-family: 'Courier New', monospace;
                            font-size: 12px; white-space: pre-wrap; color: #333;
                            border: 1px solid #e0e0e0;">
                    {escaped_diff}
                </div>"""

            cols_html = f'<br><small style="color:#888">{cols} col.</small>' if cols is not None else ''

            rows_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; vertical-align: top;">
                    <strong>{name}</strong>{cols_html}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;
                           text-align: center; vertical-align: top;">
                    {fp_api_html}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;
                           text-align: center; vertical-align: top;">
                    {fp_ul_html}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; vertical-align: top;">
                    {cause}
                    {diff_html}
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #e65100; font-size: 16px; margin-bottom: 12px;">
                🔒 Tables Bloquées — Structure Modifiée
            </h3>
            <p style="color: #555; font-size: 13px; margin-bottom: 12px;">
                Ces tables ne peuvent pas être importées tant que leur structure
                n'a pas été vérifiée et validée.
            </p>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #fff3e0;">
                        <th style="padding: 10px; text-align: left;
                                   border-bottom: 2px solid #ffe0b2;">Table</th>
                        <th style="padding: 10px; text-align: center;
                                   border-bottom: 2px solid #ffe0b2;">FP API</th>
                        <th style="padding: 10px; text-align: center;
                                   border-bottom: 2px solid #ffe0b2;">FP Config</th>
                        <th style="padding: 10px; text-align: left;
                                   border-bottom: 2px solid #ffe0b2;">Cause probable</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """

    @classmethod
    def _render_error_tables(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu du tableau des tables en erreur."""
        rows_html = ''
        for t in tables:
            name = t.get('table_name', 'unknown')
            error = cls.escape_html(t.get('error', ''))
            rows_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;
                           vertical-align: top; width: 200px;">
                    <strong>{name}</strong>
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;
                           font-family: 'Courier New', monospace; font-size: 13px;
                           color: #c62828;">
                    {error}
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #c62828; font-size: 16px; margin-bottom: 12px;">
                ⚠ Tables en Erreur
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #ffebee;">
                        <th style="padding: 10px; text-align: left;
                                   border-bottom: 2px solid #ffcdd2;">Table</th>
                        <th style="padding: 10px; text-align: left;
                                   border-bottom: 2px solid #ffcdd2;">Erreur</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """
