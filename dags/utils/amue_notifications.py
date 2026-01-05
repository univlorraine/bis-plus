"""
Fonctions de notification pour les DAGs AMUE
"""
from datetime import datetime
from typing import Dict, List
from airflow.sdk import Variable
from airflow.utils.email import send_email
import json


def send_failure_notification(context) -> None:
    """
    Callback pour envoyer une notification en cas d'échec du DAG

    Args:
        context: Contexte Airflow contenant task_instance, exception, etc.
    """
    print("[ERROR_CALLBACK] Envoi notification echec")

    task_instance = context.get('task_instance')
    exception = context.get('exception')

    recipients_var = Variable.get('amue_report_recipients', default='admin@example.com')
    recipients = [r.strip() for r in recipients_var.split(',')]

    error_info = {
        'execution_date': datetime.now().isoformat(),
        'dag_id': task_instance.dag_id if task_instance else 'unknown',
        'task_id': task_instance.task_id if task_instance else 'unknown',
        'error': str(exception) if exception else 'Unknown error',
        'status': 'failed'
    }

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #f44336; color: white; padding: 20px; }}
            .error {{ background-color: #ffebee; padding: 15px; margin: 20px 0; border-left: 4px solid #f44336; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>ERREUR Import AMUE</h1>
            <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        <div class="error">
            <h2>Erreur detectee</h2>
            <p><strong>DAG:</strong> {error_info['dag_id']}</p>
            <p><strong>Task:</strong> {error_info['task_id']}</p>
            <p><strong>Message:</strong></p>
            <pre>{error_info['error']}</pre>
        </div>
        <p>Verifiez les logs Airflow pour plus de details.</p>
    </body>
    </html>
    """

    try:
        send_email(
            to=recipients,
            subject=f"[ERREUR] Import AMUE - {datetime.now().strftime('%Y-%m-%d')}",
            html_content=html
        )
        print("[ERROR_CALLBACK] Email envoye")
    except Exception as e:
        print(f"[WARN] Erreur envoi email: {e}")

    # Sauvegarde le rapport d'erreur
    try:
        Variable.set('last_import_report', json.dumps(error_info))
    except:
        pass