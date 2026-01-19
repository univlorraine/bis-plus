# amue/notifications/templates/error.py
"""Template pour les notifications d'erreur"""
from typing import Dict, Any

from amue.notifications.templates.base import BaseTemplate


class ErrorTemplate(BaseTemplate):
    """Template pour les emails d'erreur"""

    @property
    def header_color(self) -> str:
        return "linear-gradient(135deg, #f44336 0%, #d32f2f 100%)"

    def render_content(self, context: Dict[str, Any]) -> str:
        """
        Rendu du contenu d'erreur

        Context attendu:
            - dag_id: ID du DAG
            - task_id: ID de la tâche
            - error_type: Type d'erreur
            - error_message: Message d'erreur
            - execution_date: Date d'exécution
            - status: Statut (failed, etc.)
        """
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'UnknownError')
        error_message = self.escape_html(context.get('error_message', ''))
        status = context.get('status', 'failed')

        return f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336; padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur Detectee
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG:</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tache:</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type d'erreur:</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut:</div>
                <div class="info-value">
                    <span class="badge badge-error">{status}</span>
                </div>
            </div>

            <div style="margin-top: 20px;">
                <strong>Message d'erreur:</strong>
                <div class="message-box">{error_message}</div>
            </div>
        </div>
        """
