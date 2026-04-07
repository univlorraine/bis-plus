# amue/notifications/notifier.py
"""Service de notification pour les DAGs AMUE."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from common.notifications.base_notifier import BaseNotificationService
from common.notifications.email_service import EmailService
from amue.notifications.templates import NotificationTemplates
from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class NotificationService(BaseNotificationService):
    """Service unifié de notification par email pour les DAGs AMUE."""

    SYSTEM_NAME = 'AMUE'
    DEFAULT_DAG_ID = 'amue_multi_table_import'
    TEMPLATES_CLASS = NotificationTemplates

    def _load_recipients(self) -> List[str]:
        """Charge la liste des destinataires depuis les variables Airflow."""
        recipients_var = VarMgr.get('amue_report_recipients', default=None)
        if not recipients_var:
            logger.warning(
                "Variable Airflow 'amue_report_recipients' non configurée"
                " — les notifications email AMUE ne seront pas envoyées"
            )
            return []
        recipients = [r.strip() for r in recipients_var.split(',') if r.strip()]
        logger.debug(f"Destinataires chargés: {recipients}")
        return recipients

    def _build_error_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email d'erreur."""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', 'unknown')
        return f"[ERREUR] Import AMUE - {dag_id} - {date_str}"

    def _build_success_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email de succès."""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        tables_count = len(context.get('tables_imported', []))
        total_rows = context.get('total_rows', 0)
        return f"[SUCCÈS] Import AMUE - {tables_count} table(s) - {total_rows:,} nouvelles lignes - {date_str}"
