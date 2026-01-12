"""
Générateur de rapports et notifications
"""
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Dict
from airflow.sdk import Variable


class AMUEReportGenerator:
    """Génère des rapports d'exécution et envoie des notifications"""

    def generate_report(self, insert_results: List[Dict],
                       history_result: Dict, polling_result: Dict) -> Dict:
        """Génère un rapport d'exécution"""
        print("[REPORT] Génération")

        total_tables = len(insert_results)
        total_rows = sum(r.get('rows_inserted', 0) for r in insert_results)

        report = {
            'execution_date': datetime.now().isoformat(),
            'polling_attempts': polling_result.get('attempts', 0),
            'polling_wait_minutes': polling_result.get('total_wait_minutes', 0),
            'total_tables': total_tables,
            'total_rows': total_rows,
            'tables_detail': list(insert_results),
            'history_dates': history_result.get('dates_checked', []),
            'status': 'success'
        }

        self._print_report(report)
        self._save_report(report)

        return report

    def send_notification(self, report: Dict) -> None:
        """Envoie une notification par email"""
        print("[EMAIL] Envoi notification")

        recipients = self._get_recipients()
        html = self._build_email_html(report)
        subject = f"[OK] Import AMUE - {datetime.now().strftime('%Y-%m-%d')}"

        try:
            self._send_email_smtp(
                to=recipients,
                subject=subject,
                html_content=html
            )
            print("[EMAIL] Envoyé")
        except Exception as e:
            print(f"[WARN] Email: {e}")

    def _send_email_smtp(self, to: List[str], subject: str, html_content: str) -> None:
        """Envoie un email via SMTP directement"""
        # Récupère la configuration SMTP depuis Airflow
        try:
            smtp_host = Variable.get('smtp_host', default='mailhog')
            smtp_port = int(Variable.get('smtp_port', default='1025'))
            smtp_from = Variable.get('smtp_mail_from', default='airflow@amue-project.local')
        except:
            # Valeurs par défaut si variables non configurées
            smtp_host = 'mailhog'
            smtp_port = 1025
            smtp_from = 'airflow@amue-project.local'

        # Création du message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = smtp_from
        msg['To'] = ', '.join(to)

        # Attacher le HTML
        part = MIMEText(html_content, 'html')
        msg.attach(part)

        # Envoi
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.sendmail(smtp_from, to, msg.as_string())
        server.quit()

    def _print_report(self, report: Dict) -> None:
        """Affiche le rapport dans les logs"""
        print(f"""
+================================================================+
|                    RAPPORT IMPORT AMUE                         |
+================================================================+
| Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                                  |
| Polling: {report['polling_wait_minutes']}min ({report['polling_attempts']} tentatives)              |
| Tables: {report['total_tables']}                                                  |
| Lignes: {report['total_rows']:,}                                              |
+================================================================+
        """)

        for r in report['tables_detail']:
            print(f"[OK] {r['table_name']:15} | {r['rows_inserted']:>8} lignes | {r['import_type']}")

    def _save_report(self, report: Dict) -> None:
        """Sauvegarde le rapport dans les variables"""
        try:
            from airflow.sdk.definitions.variable import Variable as SdkVariable
            SdkVariable.set('last_import_report', json.dumps(report))
        except:
            try:
                Variable.set('last_import_report', json.dumps(report))
            except:
                pass

    def _get_recipients(self) -> List[str]:
        """Récupère la liste des destinataires"""
        recipients_var = Variable.get('amue_report_recipients', default='admin@example.com')
        return [r.strip() for r in recipients_var.split(',')]

    def _build_email_html(self, report: Dict) -> str:
        """Construit le contenu HTML de l'email"""
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; }}
                th {{ background-color: #4CAF50; color: white; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Import AMUE Réussi</h1>
                <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <h2>Résumé</h2>
            <ul>
                <li>Tables: {report['total_tables']}</li>
                <li>Lignes: {report['total_rows']:,}</li>
                <li>Polling: {report['polling_wait_minutes']}min</li>
            </ul>
            <h2>Détails</h2>
            <table>
                <tr><th>Table</th><th>Lignes</th><th>Type</th></tr>
        """

        for r in report['tables_detail']:
            html += f"<tr><td>{r['table_name']}</td><td>{r['rows_inserted']:,}</td><td>{r['import_type']}</td></tr>"

        html += """
            </table>
        </body>
        </html>
        """

        return html