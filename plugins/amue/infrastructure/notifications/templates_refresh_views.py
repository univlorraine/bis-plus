# amue/notifications/templates_refresh_views.py
"""Layer: infrastructure

Template d'email pour les rapports de rafraîchissement des vues custom."""
import re
from typing import Any, Dict, List

from common.infrastructure.notifications.base_templates import BaseTemplates


class RefreshViewsTemplates(BaseTemplates):
    """Template HTML pour les notifications de rafraîchissement des vues custom."""

    @classmethod
    def render_refresh_views_success(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour une notification de rafraîchissement réussi.

        Contexte attendu :
            title           : titre de l'email
            subtitle        : date d'exécution
            dag_id          : ID du DAG ('amue_refresh_views')
            target_schema   : schéma cible (ex. 'splus_blue')
            ok              : nombre de vues créées avec succès
            ko              : nombre de vues en échec
            files_processed : liste des fichiers traités avec succès
        """
        title = context.get('title', 'Rafraîchissement Vues Réussi')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        target_schema = context.get('target_schema', '?')
        ok = context.get('ok', 0)
        ko = context.get('ko', 0)
        files_processed: List[str] = context.get('files_processed', [])
        files_failed: List[Dict[str, str]] = context.get('files_failed', [])
        total = ok + ko

        is_partial = ko > 0

        # --- Compteurs (table au lieu de flex pour la compatibilité email) ---
        counter_ok_color = '#2e7d32'
        counter_ko_color = '#c62828' if ko > 0 else '#999'

        counters_html = f"""
        <table style="width: 100%; border-collapse: separate; border-spacing: 12px 0;
                      margin: 20px 0;">
            <tr>
                <td style="width: 33%; text-align: center; background: #fff;
                           padding: 16px 8px; border-radius: 6px;
                           border: 1px solid #e3f2fd;">
                    <div style="font-size: 28px; font-weight: bold;
                                color: {counter_ok_color};">{ok}</div>
                    <div style="font-size: 12px; color: #666; margin-top: 4px;">
                        vue(s) OK
                    </div>
                </td>
                <td style="width: 33%; text-align: center; background: #fff;
                           padding: 16px 8px; border-radius: 6px;
                           border: 1px solid #e3f2fd;">
                    <div style="font-size: 28px; font-weight: bold;
                                color: {counter_ko_color};">{ko}</div>
                    <div style="font-size: 12px; color: #666; margin-top: 4px;">
                        vue(s) en échec
                    </div>
                </td>
                <td style="width: 33%; text-align: center; background: #fff;
                           padding: 16px 8px; border-radius: 6px;
                           border: 1px solid #e3f2fd;">
                    <div style="font-size: 28px; font-weight: bold;
                                color: #1565C0;">{total}</div>
                    <div style="font-size: 12px; color: #666; margin-top: 4px;">
                        total
                    </div>
                </td>
            </tr>
        </table>
        """

        # --- Tableau des vues créées ---
        if files_processed:
            rows = ''.join(
                f"""<tr>
                    <td style="padding: 8px 12px; border-bottom: 1px solid #eee;">
                        <code style="font-size: 12px;">{cls._view_name(f)}</code>
                    </td>
                    <td style="padding: 8px 12px; border-bottom: 1px solid #eee;
                               color: #888; font-size: 12px;">
                        {f}
                    </td>
                    <td style="padding: 8px 12px; border-bottom: 1px solid #eee;
                               text-align: center;">
                        <span class="badge badge-success">OK</span>
                    </td>
                </tr>"""
                for f in files_processed
            )
            views_table = f"""
            <table style="width: 100%; border-collapse: collapse; margin-top: 12px;
                          font-size: 13px;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="text-align: left; padding: 8px 12px;
                                   font-size: 11px; text-transform: uppercase;
                                   letter-spacing: 1px; color: #666; border-bottom: 2px solid #e0e0e0;">
                            Vue
                        </th>
                        <th style="text-align: left; padding: 8px 12px;
                                   font-size: 11px; text-transform: uppercase;
                                   letter-spacing: 1px; color: #666; border-bottom: 2px solid #e0e0e0;">
                            Fichier SQL
                        </th>
                        <th style="text-align: center; padding: 8px 12px;
                                   font-size: 11px; text-transform: uppercase;
                                   letter-spacing: 1px; color: #666; border-bottom: 2px solid #e0e0e0;">
                            Statut
                        </th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
            """
        else:
            views_table = (
                '<p style="color: #999; font-size: 13px; margin-top: 8px;">'
                'Aucune vue créée avec succès.</p>'
            )

        # --- Bloc erreurs partielles ---
        ko_block = ''
        if ko > 0:
            if files_failed:
                failed_rows = ''.join(
                    f"""<tr>
                        <td style="padding: 8px 12px; border-bottom: 1px solid #ffd0b0;
                                   font-size: 12px;">
                            <code>{cls._view_name(f['filename'])}</code>
                            <div style="color: #888; font-size: 11px; margin-top: 2px;">
                                {f['filename']}
                            </div>
                        </td>
                        <td style="padding: 8px 12px; border-bottom: 1px solid #ffd0b0;
                                   font-size: 12px; color: #c62828; word-break: break-word;">
                            {cls.escape_html(f.get('error', 'Erreur inconnue'))}
                        </td>
                    </tr>"""
                    for f in files_failed
                )
                failed_table = f"""
                <table style="width: 100%; border-collapse: collapse; margin-top: 12px;
                              font-size: 13px;">
                    <thead>
                        <tr style="background: #ffe0b2;">
                            <th style="text-align: left; padding: 8px 12px;
                                       font-size: 11px; text-transform: uppercase;
                                       letter-spacing: 1px; color: #bf360c;
                                       border-bottom: 2px solid #ffb74d;">
                                Vue
                            </th>
                            <th style="text-align: left; padding: 8px 12px;
                                       font-size: 11px; text-transform: uppercase;
                                       letter-spacing: 1px; color: #bf360c;
                                       border-bottom: 2px solid #ffb74d;">
                                Erreur
                            </th>
                        </tr>
                    </thead>
                    <tbody>{failed_rows}</tbody>
                </table>
                """
            else:
                failed_table = (
                    '<p style="color: #888; font-size: 13px; margin-top: 8px;">'
                    'Détail des erreurs non disponible — consultez les logs Airflow.</p>'
                )

            ko_block = f"""
            <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                        padding: 15px 20px; border-radius: 4px; margin-top: 20px;">
                <strong style="color: #e65100; font-size: 15px;">
                    {ko} vue(s) en erreur
                </strong>
                {failed_table}
            </div>
            """

        # --- Bloc principal ---
        status_badge = (
            '<span class="badge badge-warning">partiel</span>'
            if is_partial else
            '<span class="badge badge-success">succès</span>'
        )
        border_color = '#FF9800' if is_partial else '#2196F3'
        header_color = cls.HEADER_COLOR_WARNING if is_partial else cls.HEADER_COLOR_SYNC

        content = f"""
        <div style="background: #f3f8ff; border-left: 4px solid {border_color};
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #1565C0; margin-top: 0; font-size: 18px;">
                Rafraîchissement des vues custom
            </h2>
            <p style="color: #555; margin-top: 8px;">
                Les vues custom du schéma <code>splus</code> ont été recréées
                et pointent vers <code>{target_schema}</code>.
            </p>

            {counters_html}

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Schéma cible :</div>
                <div class="info-value"><code>{target_schema}</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">{status_badge}</div>
            </div>

            <div style="margin-top: 20px;">
                <strong style="font-size: 14px; color: #333;">
                    Vues créées ({ok}) :
                </strong>
                {views_table}
            </div>
        </div>

        {ko_block}
        """

        header = cls._render_header(title, subtitle, header_color)
        footer = cls._render_footer(
            show_actions=ko > 0,
            actions=(
                "- Consultez les logs de <code>refresh_custom_views</code> pour identifier les vues en erreur<br>"
                "- Vérifiez la syntaxe SQL des fichiers concernés dans <code>scripts/sql/custom_views/</code><br>"
                "- Relancez <code>amue_refresh_views</code> une fois les fichiers corrigés"
            ) if ko > 0 else '',
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def render_switch_custom_views_ko(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour une alerte de vues custom en échec lors du switch.

        Contexte attendu :
            title         : titre de l'email
            subtitle      : date d'exécution
            dag_id        : ID du DAG ('amue_multi_table_import')
            target_schema : schéma cible (ex. 'splus_blue')
            ko            : nombre de vues en échec
            files_failed  : liste de dicts {"filename": ..., "error": ...}
        """
        title = context.get('title', 'Vues custom en échec')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        target_schema = context.get('target_schema', '?')
        ko = context.get('ko', 0)
        files_failed: List[Dict[str, str]] = context.get('files_failed', [])

        if files_failed:
            failed_rows = ''.join(
                f"""<tr>
                    <td style="padding: 8px 12px; border-bottom: 1px solid #ffd0b0; font-size: 12px;">
                        <code>{cls._view_name(f['filename'])}</code>
                        <div style="color: #888; font-size: 11px; margin-top: 2px;">{f['filename']}</div>
                    </td>
                    <td style="padding: 8px 12px; border-bottom: 1px solid #ffd0b0;
                               font-size: 12px; color: #c62828; word-break: break-word;">
                        {cls.escape_html(f.get('error', 'Erreur inconnue'))}
                    </td>
                </tr>"""
                for f in files_failed
            )
            failed_table = f"""
            <table style="width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px;">
                <thead>
                    <tr style="background: #ffe0b2;">
                        <th style="text-align: left; padding: 8px 12px; font-size: 11px;
                                   text-transform: uppercase; letter-spacing: 1px; color: #bf360c;
                                   border-bottom: 2px solid #ffb74d;">Vue</th>
                        <th style="text-align: left; padding: 8px 12px; font-size: 11px;
                                   text-transform: uppercase; letter-spacing: 1px; color: #bf360c;
                                   border-bottom: 2px solid #ffb74d;">Erreur</th>
                    </tr>
                </thead>
                <tbody>{failed_rows}</tbody>
            </table>
            """
        else:
            failed_table = (
                '<p style="color: #888; font-size: 13px; margin-top: 8px;">'
                'Détail des erreurs non disponible — consultez les logs Airflow.</p>'
            )

        content = f"""
        <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #e65100; margin-top: 0; font-size: 18px;">
                {ko} vue(s) custom non basculée(s)
            </h2>
            <p style="color: #555; margin-top: 8px;">
                L'import a réussi, mais {ko} vue(s) custom n'ont pas pu être
                recréées vers <code>{target_schema}</code> lors du switch.
                Ces vues pointent encore vers l'ancien schéma.
            </p>

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Schéma cible :</div>
                <div class="info-value"><code>{target_schema}</code></div>

                <div class="info-label">Vues en échec :</div>
                <div class="info-value">
                    <span class="badge badge-warning">{ko} KO</span>
                </div>
            </div>

            {failed_table}
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_WARNING)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Consultez les logs de <code>switch_views</code> pour le détail des erreurs<br>"
                "- Corrigez les fichiers SQL dans <code>scripts/sql/custom_views/</code><br>"
                "- Relancez <code>amue_refresh_views</code> pour rebâtir les vues custom"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @staticmethod
    def _view_name(filename: str) -> str:
        """
        Dérive le nom de la vue depuis le nom de fichier SQL.

        Exemples :
            '10_v_mon_rapport.sql' → 'v_mon_rapport'
            'v_check_auth.sql'     → 'v_check_auth'
            'my_view.sql'          → 'my_view'
        """
        name = re.sub(r'\.sql$', '', filename, flags=re.IGNORECASE)
        name = re.sub(r'^\d+_', '', name)
        return name
