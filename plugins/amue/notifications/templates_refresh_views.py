# amue/notifications/templates_refresh_views.py
"""Template d'email pour les rapports de rafraîchissement des vues custom."""
from typing import Any, Dict, List

from amue.notifications.templates_base import BaseTemplates


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

        # Couleur du bandeau statut selon résultat
        status_color = "#4CAF50" if ko == 0 else "#FF9800"
        status_label = "succès" if ko == 0 else "partiel"

        # Liste des vues créées
        if files_processed:
            rows = "".join(
                f'<tr><td style="padding: 6px 12px; border-bottom: 1px solid #eee;">'
                f'<code>{f}</code></td></tr>'
                for f in files_processed
            )
            views_table = f"""
            <table style="width: 100%; border-collapse: collapse; margin-top: 12px;
                          font-size: 13px;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="text-align: left; padding: 8px 12px;
                                   font-size: 11px; text-transform: uppercase;
                                   letter-spacing: 1px; color: #666;">
                            Fichier SQL
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

        ko_block = ""
        if ko > 0:
            ko_block = f"""
            <div style="background: #fff3e0; border: 1px solid #ffe082; padding: 12px 16px;
                        border-radius: 4px; margin-top: 16px; font-size: 14px; color: #555;">
                <strong>⚠ Attention :</strong> {ko} vue(s) ont échoué.
                Consultez les logs Airflow pour le détail des erreurs.
            </div>
            """

        content = f"""
        <div style="background: #f3f8ff; border-left: 4px solid #2196F3;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #1565C0; margin-top: 0; font-size: 18px;">
                Rafraîchissement des vues custom
            </h2>
            <p style="color: #555; margin-top: 8px;">
                Les vues custom du schéma <code>splus</code> ont été recréées
                et pointent vers <code>{target_schema}</code>.
            </p>

            <!-- Compteurs -->
            <div style="display: flex; gap: 16px; margin: 20px 0;">
                <div style="flex: 1; text-align: center; background: #fff;
                            padding: 16px; border-radius: 6px;
                            border: 1px solid #e3f2fd;">
                    <div style="font-size: 28px; font-weight: bold; color: #2e7d32;">
                        {ok}
                    </div>
                    <div style="font-size: 12px; color: #666; margin-top: 4px;">
                        vue(s) OK
                    </div>
                </div>
                <div style="flex: 1; text-align: center; background: #fff;
                            padding: 16px; border-radius: 6px;
                            border: 1px solid #e3f2fd;">
                    <div style="font-size: 28px; font-weight: bold;
                                color: {'#c62828' if ko > 0 else '#999'};">
                        {ko}
                    </div>
                    <div style="font-size: 12px; color: #666; margin-top: 4px;">
                        vue(s) en échec
                    </div>
                </div>
            </div>

            <div class="info-grid">
                <div class="info-label">Schéma cible :</div>
                <div class="info-value"><code>{target_schema}</code></div>

                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span style="background: {status_color}; color: #fff;
                                 padding: 2px 8px; border-radius: 3px;
                                 font-size: 12px;">{status_label}</span>
                </div>
            </div>

            <div style="margin-top: 20px;">
                <strong style="font-size: 14px; color: #333;">
                    Vues créées ({ok}) :
                </strong>
                {views_table}
            </div>

            {ko_block}
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_SYNC)
        footer = cls._render_footer()
        return cls._wrap_html(header, content, footer)
