# amue/notifications/report_generator.py
"""
Générateur de rapports d'import AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module génère le rapport final après un import réussi. Il est appelé
par la dernière task du DAG (send_report) pour :
    1. Agréger les statistiques d'import
    2. Afficher le rapport dans les logs Airflow
    3. Sauvegarder le rapport dans les variables Airflow
    4. Envoyer une notification email de succès

================================================================================
CONTENU DU RAPPORT
================================================================================

Le rapport inclut :

MÉTRIQUES GLOBALES :
    - Date et heure d'exécution
    - Durée totale (depuis le début du polling)
    - Nombre de tentatives de polling
    - Temps d'attente du polling

STATISTIQUES D'IMPORT :
    - Nombre de tables importées
    - Nombre de tables ignorées (0 lignes)
    - Total de lignes récupérées de l'API
    - Total de lignes insérées en base

DÉTAIL PAR TABLE :
    - Nom de la table
    - Lignes récupérées / insérées
    - Type d'import (full / differential)
    - Statut (success / error)
    - Fingerprint (tronqué)

================================================================================
FORMAT DU RAPPORT DANS LES LOGS
================================================================================

    ======================================================================
                        RAPPORT IMPORT AMUE
    ======================================================================
      Date d'exécution : 2024-01-15 10:30:00
      Durée totale     : 45m 30s
      Polling          : 3 tentative(s), 30.0min d'attente
    ----------------------------------------------------------------------
      Tables traitées  : 5
      Tables ignorées  : 2 (0 lignes)
      Lignes récupérées: 150,000
      Lignes insérées  : 150,000
    ======================================================================

      DÉTAIL PAR TABLE:
    ----------------------------------------------------------------------
      Table           |     Récup. |     Inséré | Type         | Statut
    ----------------------------------------------------------------------
      CSKS            |     15,000 |     15,000 | full         | [OK]
      COST            |     50,000 |     50,000 | differential | [OK]
      ...

================================================================================
PERSISTANCE
================================================================================

Le rapport est archivé en fichier JSON dans le répertoire de logs Airflow
(/opt/airflow/logs/reports/). Cela permet :
    - De consulter les rapports via les logs Airflow
    - D'intégrer les rapports dans un système de monitoring
    - De garder un historique horodaté

================================================================================
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from amue.notifications.notifier import NotificationService
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class AMUEReportGenerator:
    """
    Génère des rapports d'exécution et envoie des notifications
    """

    def __init__(self):
        """Initialise le générateur de rapports"""
        self.notification_service = NotificationService()

    def generate_report(self, import_results: List[Dict], polling_result: Dict,
                        title: str = 'RAPPORT IMPORT AMUE') -> Dict:
        """
        Génère un rapport d'exécution

        Args:
            import_results: Résultats des imports
            polling_result: Résultat du polling (avec start_time)
            title: Titre affiché dans les logs (défaut: 'RAPPORT IMPORT AMUE')

        Returns:
            Rapport complet
        """
        logger.info("[REPORT] Génération du rapport")

        # Statistiques
        tables_skipped = sum(1 for r in import_results if r.get('rows_fetched', 0) == 0)
        total_tables = len(import_results) - tables_skipped
        total_inserted = sum(r.get('rows_inserted', 0) for r in import_results)
        total_updated = sum(r.get('rows_updated', 0) for r in import_results)
        total_fetched = sum(r.get('rows_fetched', 0) for r in import_results)

        # Calcul de la durée depuis le début du DAG
        duration = self._calculate_duration(polling_result)

        # Enrichit les détails des tables
        tables_detail = []
        for r in import_results:
            tables_detail.append({
                'table_name': r.get('table_name', 'unknown'),
                'rows_fetched': r.get('rows_fetched', 0),
                'rows_inserted': r.get('rows_inserted', 0),
                'rows_updated': r.get('rows_updated', 0),
                'import_type': r.get('import_type', 'full'),
                'status': r.get('status', 'success'),
                'fingerprint_API': r.get('fingerprint_API', '')[:16] + '...' if r.get('fingerprint_API') else '',
                'fingerprint_UL': r.get('fingerprint_UL', '')[:16] + '...' if r.get('fingerprint_UL') else ''
            })

        report = {
            'execution_date': datetime.now().isoformat(),
            'start_time': polling_result.get('start_time', ''),
            'duration': duration,
            'polling_attempts': polling_result.get('attempts', 0),
            'polling_wait_minutes': round(polling_result.get('total_wait_minutes', 0), 1),
            'total_tables': total_tables,
            'tables_skipped': tables_skipped,
            'total_fetched': total_fetched,
            'total_inserted': total_inserted,
            'total_updated': total_updated,
            'tables_detail': tables_detail,
            'status': 'success'
        }

        self._print_report(report, title=title)
        self._save_report(report)

        return report

    def _calculate_duration(self, polling_result: Dict) -> str:
        """Calcule la durée totale depuis le début"""
        start_time_str = polling_result.get('start_time')

        if start_time_str:
            try:
                start_dt = datetime.fromisoformat(start_time_str)
                duration_seconds = (datetime.now() - start_dt).total_seconds()
                return self._format_duration(duration_seconds)
            except Exception as e:
                logger.warning(f"[REPORT] Erreur calcul durée: {e}")

        # Fallback sur le temps de polling
        wait_minutes = polling_result.get('total_wait_minutes', 0)
        if wait_minutes > 0:
            return f"{wait_minutes:.1f}min (polling uniquement)"

        return "N/A"

    def send_notification(self, report: Dict) -> None:
        """
        Envoie une notification par email

        Args:
            report: Rapport à envoyer
        """
        logger.info("[REPORT] Envoi notification email")

        # Prépare les données pour le service de notification
        notification_data = {
            'dag_id': 'amue_multi_table_import',
            'execution_date': report.get('execution_date', datetime.now().isoformat()),
            'duration': report.get('duration', 'N/A'),
            'tables_imported': report.get('tables_detail', []),
            'total_rows': report.get('total_inserted', 0),
            'total_fetched': report.get('total_fetched', 0)
        }

        success = self.notification_service.notify_success(notification_data)

        if success:
            logger.info("[REPORT] Email envoyé avec succès")
        else:
            logger.warning("[REPORT] Échec envoi email")

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

    def _print_report(self, report: Dict, title: str = 'RAPPORT IMPORT AMUE') -> None:
        """Affiche le rapport dans les logs"""
        logger.info("=" * 70)
        logger.info(f"                    {title}")
        logger.info("=" * 70)
        logger.info(f"  Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  Durée totale     : {report.get('duration', 'N/A')}")
        logger.info(f"  Polling          : {report.get('polling_attempts', 0)} tentative(s), "
                    f"{report.get('polling_wait_minutes', 0)}min d'attente")
        logger.info("-" * 70)
        logger.info(f"  Tables traitées  : {report['total_tables']}")
        logger.info(f"  Lignes récupérées  : {report['total_fetched']:,}")
        logger.info(f"  Lignes insérées    : {report['total_inserted']:,}  (nouvelles)")
        logger.info(f"  Lignes mises à jour: {report['total_updated']:,}  (existantes)")
        logger.info("=" * 70)

        if report['tables_detail']:
            logger.info("")
            logger.info("  DÉTAIL PAR TABLE:")
            logger.info("-" * 80)
            logger.info(f"  {'Table':<15} | {'Récup.':>10} | {'Insérées':>10} | {'MAJ':>10} | {'Type':<12} | Statut")
            logger.info("-" * 80)

            for t in report['tables_detail']:
                status_icon = "OK" if t.get('status') == 'success' else "!!"
                logger.info(
                    f"  {t['table_name']:<15} | "
                    f"{t.get('rows_fetched', 0):>10,} | "
                    f"{t.get('rows_inserted', 0):>10,} | "
                    f"{t.get('rows_updated', 0):>10,} | "
                    f"{t.get('import_type', 'full'):<12} | "
                    f"[{status_icon}]"
                )

            logger.info("-" * 80)
        logger.info("")

    def _save_report(self, report: Dict) -> None:
        """Sauvegarde le rapport en fichier JSON."""
        # Archivage en fichier JSON
        try:
            reports_dir = Path(VarMgr.get('amue_reports_dir', default='/opt/airflow/logs/reports'))
            reports_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = reports_dir / f"import_report_{timestamp}.json"
            filepath.write_text(json.dumps(report, default=str, indent=2), encoding='utf-8')
            logger.info(f"[REPORT] Rapport archivé: {filepath}")
        except Exception as e:
            logger.warning(f"[REPORT] Impossible d'archiver le rapport en fichier: {e}")

    def generate_and_send(self, import_results: List[Dict], polling_result: Dict,
                          title: str = 'RAPPORT IMPORT AMUE') -> Dict:
        """
        Génère le rapport et envoie la notification en une seule opération

        Args:
            import_results: Résultats des imports
            polling_result: Résultat du polling
            title: Titre affiché dans les logs (défaut: 'RAPPORT IMPORT AMUE')

        Returns:
            Rapport généré avec statut d'envoi
        """
        # Génère le rapport
        report = self.generate_report(import_results, polling_result, title=title)

        # Envoie la notification
        self.send_notification(report)

        return report
