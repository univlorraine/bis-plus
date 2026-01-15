# amue/notifications/report_generator.py
"""
Générateur de rapports et notifications
Utilise le nouveau système de notifications
"""
import json
from datetime import datetime
from typing import List, Dict
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr
from amue.utils.logger import get_logger
from amue.notifications.notifiers.success_notifier import SuccessNotifier

logger = get_logger(__name__)


class AMUEReportGenerator:
    """
    Génère des rapports d'exécution et envoie des notifications

    Utilise SuccessNotifier pour l'envoi des emails.
    """

    def __init__(self):
        """Initialise le générateur de rapports"""
        self.notifier = SuccessNotifier()

    def generate_report(self, insert_results: List[Dict],
                       history_result: Dict, polling_result: Dict) -> Dict:
        """
        Génère un rapport d'exécution

        Args:
            insert_results: Résultats des imports
            history_result: Résultat de la vérification historique
            polling_result: Résultat du polling

        Returns:
            Rapport complet
        """
        logger.info("Génération du rapport")

        total_tables = len(insert_results)
        total_rows = sum(r.get('rows_inserted', 0) for r in insert_results)

        # Calcul de la durée
        start_time = polling_result.get('start_time')
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                duration_seconds = (datetime.now() - start_dt).total_seconds()
                duration = self._format_duration(duration_seconds)
            except Exception:
                duration = f"{polling_result.get('total_wait_minutes', 0)}min"
        else:
            duration = f"{polling_result.get('total_wait_minutes', 0)}min"

        report = {
            'execution_date': datetime.now().isoformat(),
            'polling_attempts': polling_result.get('attempts', 0),
            'polling_wait_minutes': polling_result.get('total_wait_minutes', 0),
            'duration': duration,
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
        """
        Envoie une notification par email

        Args:
            report: Rapport à envoyer
        """
        logger.info("Envoi notification email")

        # Prépare les données pour le notifier
        notification_data = {
            'dag_id': 'amue_multi_table_import',
            'execution_date': report.get('execution_date', datetime.now().isoformat()),
            'duration': report.get('duration', 'N/A'),
            'tables_imported': report.get('tables_detail', []),
            'total_rows': report.get('total_rows', 0)
        }

        success = self.notifier.notify(notification_data)

        if success:
            logger.info("Email envoyé avec succès")
        else:
            logger.warning("Échec envoi email")

    def _format_duration(self, seconds: float) -> str:
        """Formate une durée en secondes en format lisible"""
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"

    def _print_report(self, report: Dict) -> None:
        """Affiche le rapport dans les logs"""
        logger.info(f"""
+================================================================+
|                    RAPPORT IMPORT AMUE                         |
+================================================================+
| Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                                  |
| Durée: {report.get('duration', 'N/A')}                                              |
| Tables: {report['total_tables']}                                                  |
| Lignes: {report['total_rows']:,}                                              |
+================================================================+
        """)

        for r in report['tables_detail']:
            status_icon = "[OK]" if r.get('status') == 'success' else "[!!]"
            logger.info(f"{status_icon} {r['table_name']:15} | {r.get('rows_inserted', 0):>8} lignes | {r.get('import_type', 'full')}")

    def _save_report(self, report: Dict) -> None:
        """Sauvegarde le rapport dans les variables"""
        VarMgr.set('last_import_report', json.dumps(report))
