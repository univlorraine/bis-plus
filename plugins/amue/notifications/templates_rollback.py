# amue/notifications/templates_rollback.py
"""Template d'email pour les rapports de rollback blue/green."""
from typing import Any, Dict

from common.notifications.base_templates import BaseTemplates


class RollbackTemplates(BaseTemplates):
    """Template HTML pour les notifications de rollback blue/green."""

    @classmethod
    def render_rollback_success(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour une notification de rollback réussi.

        Contexte attendu :
            title           : titre de l'email
            subtitle        : date d'exécution
            dag_id          : ID du DAG ('amue_rollback')
            previous_schema : schéma qui était actif avant (ex. 'splus_green')
            new_schema      : schéma maintenant actif (ex. 'splus_blue')
        """
        title = context.get('title', 'Rollback Réussi')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        previous_schema = context.get('previous_schema', '?')
        new_schema = context.get('new_schema', '?')

        content = f"""
        <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #e65100; margin-top: 0; font-size: 18px;">
                Rollback Blue/Green Effectué
            </h2>
            <p style="color: #555; margin-top: 8px;">
                Le schéma inactif a été restauré et les vues <code>splus</code>
                pointent maintenant vers le schéma précédent.
            </p>

            <!-- Schéma précédent → Schéma restauré -->
            <div style="display: flex; align-items: center; gap: 16px; margin: 24px 0;
                        background: #fff; padding: 20px; border-radius: 6px;
                        border: 1px solid #ffe0b2;">
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase;
                                letter-spacing: 1px; margin-bottom: 6px;">
                        Schéma désactivé
                    </div>
                    <code style="font-size: 15px; background: #ffebee;
                                 color: #c62828; padding: 6px 12px;">{previous_schema}</code>
                    <div style="font-size: 11px; color: #999; margin-top: 6px;">
                        → mis hors ligne
                    </div>
                </div>
                <div style="font-size: 32px; color: #FF9800; font-weight: bold;">⟵</div>
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 11px; color: #666; text-transform: uppercase;
                                letter-spacing: 1px; margin-bottom: 6px;">
                        Schéma restauré
                    </div>
                    <code style="font-size: 15px; background: #e8f5e9;
                                 color: #2e7d32; padding: 6px 12px;">{new_schema}</code>
                    <div style="font-size: 11px; color: #999; margin-top: 6px;">
                        ← vues actives
                    </div>
                </div>
            </div>

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-warning">rollback</span>
                </div>
            </div>
        </div>

        <div style="background: #fff8e1; border: 1px solid #ffe082; padding: 15px;
                    border-radius: 4px; margin-top: 20px; font-size: 14px; color: #555;">
            <strong>Information :</strong> Un nouveau rollback n'est plus possible
            jusqu'au prochain import. Vérifiez que les vues <code>splus</code> exposent
            bien les données attendues.
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ROLLBACK)
        footer = cls._render_footer()
        return cls._wrap_html(header, content, footer)
