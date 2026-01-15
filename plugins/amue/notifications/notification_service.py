"""
Utilitaires de notification pour les DAGs AMUE
Gestion centralisée des notifications d'erreur et de succès
"""
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr


@dataclass
class ErrorContext:
    """Contexte d'une erreur pour notification"""
    execution_date: str
    dag_id: str
    task_id: str
    error_message: str
    error_type: str
    status: str = 'failed'


class NotificationService:
    """
    Service centralisé de notification

    Responsabilités :
    - Construction des emails d'erreur
    - Construction des emails de succès
    - Envoi via SMTP direct
    - Sauvegarde des rapports d'erreur
    """

    def __init__(self):
        """Initialise le service de notification"""
        print("[DEBUG] NotificationService.__init__ - VERSION SMTP DIRECT")
        self.recipients = self._load_recipients()
        self.smtp_host = VarMgr.get('smtp_host', default='mailhog')
        self.smtp_port = int(VarMgr.get('smtp_port', default='1025'))
        self.smtp_from = VarMgr.get('smtp_mail_from', default='airflow@amue.local')
        print(f"[DEBUG] SMTP config: host={self.smtp_host}, port={self.smtp_port}, from={self.smtp_from}")
        print(f"[DEBUG] Recipients: {self.recipients}")

    def _load_recipients(self) -> List[str]:
        """Charge la liste des destinataires"""
        recipients_var = VarMgr.get('amue_report_recipients', default='admin@example.com')
        return [r.strip() for r in recipients_var.split(',') if r.strip()]

    def _send_email(self, subject: str, html_content: str) -> None:
        """Envoie un email via SMTP direct"""
        print(f"[DEBUG] _send_email appelé - SMTP direct vers {self.smtp_host}:{self.smtp_port}")

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.smtp_from
        msg['To'] = ', '.join(self.recipients)

        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        print(f"[DEBUG] Connexion SMTP à {self.smtp_host}:{self.smtp_port}...")
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.sendmail(self.smtp_from, self.recipients, msg.as_string())
        print("[DEBUG] Email envoyé avec succès via SMTP direct")

    def send_error_notification(self, error_context: ErrorContext) -> None:
        """
        Envoie une notification d'erreur

        Args:
            error_context: Contexte de l'erreur
        """
        print("[NOTIFICATION] Envoi notification d'erreur")

        subject = self._build_error_subject(error_context)
        html = self._build_error_html(error_context)

        try:
            self._send_email(subject, html)
            print("[NOTIFICATION] Email d'erreur envoyé avec succès")
        except Exception as e:
            print(f"[WARN] Échec envoi email d'erreur: {str(e)}")

        # Sauvegarde pour traçabilité
        self._save_error_report(error_context)

    def _build_error_subject(self, ctx: ErrorContext) -> str:
        """Construit le sujet de l'email d'erreur"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        return f"[ERREUR] Import AMUE - {ctx.dag_id} - {date_str}"

    def _build_error_html(self, ctx: ErrorContext) -> str:
        """
        Construit le contenu HTML de l'email d'erreur

        Args:
            ctx: Contexte de l'erreur

        Returns:
            HTML formaté
        """
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 800px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background: linear-gradient(135deg, #f44336 0%, #d32f2f 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 8px 8px 0 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .header h1 {{
                    margin: 0 0 10px 0;
                    font-size: 28px;
                    font-weight: 600;
                }}
                .header .date {{
                    opacity: 0.9;
                    font-size: 14px;
                }}
                .content {{
                    background: #fff;
                    padding: 30px;
                    border: 1px solid #e0e0e0;
                    border-top: none;
                    border-radius: 0 0 8px 8px;
                }}
                .error-box {{
                    background: #ffebee;
                    border-left: 4px solid #f44336;
                    padding: 20px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
                .error-box h2 {{
                    color: #c62828;
                    margin-top: 0;
                    font-size: 20px;
                }}
                .info-grid {{
                    display: grid;
                    grid-template-columns: 150px 1fr;
                    gap: 12px;
                    margin: 20px 0;
                }}
                .info-label {{
                    font-weight: 600;
                    color: #666;
                }}
                .error-message {{
                    background: #f5f5f5;
                    padding: 15px;
                    border-radius: 4px;
                    font-family: 'Courier New', monospace;
                    font-size: 13px;
                    overflow-x: auto;
                    margin: 15px 0;
                    border: 1px solid #ddd;
                }}
                .footer {{
                    margin-top: 30px;
                    padding-top: 20px;
                    border-top: 1px solid #e0e0e0;
                    font-size: 13px;
                    color: #666;
                }}
                .action-button {{
                    display: inline-block;
                    background: #2196F3;
                    color: white;
                    padding: 12px 24px;
                    text-decoration: none;
                    border-radius: 4px;
                    margin-top: 20px;
                    font-weight: 500;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>⚠️ Erreur d'Import AMUE</h1>
                <div class="date">{ctx.execution_date}</div>
            </div>
            
            <div class="content">
                <div class="error-box">
                    <h2>Erreur Détectée</h2>
                    
                    <div class="info-grid">
                        <div class="info-label">DAG:</div>
                        <div><strong>{ctx.dag_id}</strong></div>
                        
                        <div class="info-label">Tâche:</div>
                        <div><strong>{ctx.task_id}</strong></div>
                        
                        <div class="info-label">Type d'erreur:</div>
                        <div><code>{ctx.error_type}</code></div>
                        
                        <div class="info-label">Statut:</div>
                        <div><span style="color: #d32f2f; font-weight: 600;">
                            {ctx.status.upper()}
                        </span></div>
                    </div>
                    
                    <div style="margin-top: 20px;">
                        <strong>Message d'erreur:</strong>
                        <div class="error-message">{self._escape_html(ctx.error_message)}</div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>
                        <strong>Actions recommandées:</strong><br>
                        • Consultez les logs Airflow pour plus de détails<br>
                        • Vérifiez la configuration des variables Airflow<br>
                        • Contactez l'équipe de support si le problème persiste
                    </p>
                    
                    <a href="http://localhost:8080/dags/{ctx.dag_id}" class="action-button">
                        Voir dans Airflow UI →
                    </a>
                </div>
            </div>
        </body>
        </html>
        """

    def _escape_html(self, text: str) -> str:
        """Échappe les caractères HTML"""
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#x27;'))

    def _save_error_report(self, ctx: ErrorContext) -> None:
        """Sauvegarde le rapport d'erreur dans les variables"""
        report = {
            'execution_date': ctx.execution_date,
            'dag_id': ctx.dag_id,
            'task_id': ctx.task_id,
            'error': ctx.error_message,
            'error_type': ctx.error_type,
            'status': ctx.status
        }

        try:
            VarMgr.set('last_import_report', json.dumps(report))
            print("[NOTIFICATION] Rapport d'erreur sauvegardé")
        except Exception as e:
            print(f"[WARN] Échec sauvegarde rapport: {str(e)}")


# ============================================================================
# FONCTION CALLBACK POUR AIRFLOW
# ============================================================================

def send_failure_notification(context: Dict) -> None:
    """
    Callback Airflow pour envoyer une notification en cas d'échec

    Cette fonction est appelée automatiquement par Airflow quand :
    - Une tâche échoue (task-level callback)
    - Un DAG échoue (dag-level callback)

    Args:
        context: Contexte Airflow avec task_instance, exception, etc.
    """
    print("[ERROR_CALLBACK] Déclenchement du callback d'erreur - VERSION SMTP DIRECT v2")

    # Extraction du contexte
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    # Vérifie qu'il y a bien une exception avant d'envoyer
    if not exception:
        print("[ERROR_CALLBACK] Pas d'exception dans le contexte - notification ignorée")
        return

    # Construction du contexte d'erreur
    error_context = ErrorContext(
        execution_date=datetime.now().isoformat(),
        dag_id=task_instance.dag_id if task_instance else 'unknown',
        task_id=task_instance.task_id if task_instance else 'unknown',
        error_message=str(exception),
        error_type=type(exception).__name__,
        status='failed'
    )

    # Envoi de la notification
    service = NotificationService()
    service.send_error_notification(error_context)

    print("[ERROR_CALLBACK] Callback d'erreur terminé")