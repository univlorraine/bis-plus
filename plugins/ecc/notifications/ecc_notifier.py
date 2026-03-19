# ecc/notifications/ecc_notifier.py
"""Service de notification pour le DAG ECC."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from amue.notifications.email_service import EmailService, Email
from ecc.notifications.ecc_templates import ECCNotificationTemplates
from ecc.utils.config.settings import get_ecc_recipients

logger = logging.getLogger(__name__)


class ECCNotificationService:
    """
    Service de notification par email pour le DAG ECC.

    Utilise les templates ECCNotificationTemplates et charge les destinataires
    depuis la variable Airflow 'ecc_report_recipients'.
    """

    def __init__(self, email_service: Optional[EmailService] = None):
        self.email_service = email_service or EmailService()
        self.recipients = self._load_recipients()

    def _load_recipients(self) -> List[str]:
        """Charge les destinataires depuis la variable Airflow ecc_report_recipients."""
        recipients = get_ecc_recipients()
        logger.debug(f"Destinataires ECC charges: {recipients}")
        return recipients

    def notify_error(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification d'erreur ECC."""
        logger.info("[ECC] Envoi notification d'erreur")

        context = self._build_error_context(data)
        subject = self._build_error_subject(context)
        html_content = ECCNotificationTemplates.render_error(context)

        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def notify_success(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification de succes ECC."""
        logger.info("[ECC] Envoi notification de succes")

        context = self._build_success_context(data)
        subject = self._build_success_subject(context)
        html_content = ECCNotificationTemplates.render_success(context)

        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def _build_error_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template d'erreur ECC."""
        task_instance = data.get('task_instance')
        exception = data.get('exception')

        if task_instance:
            dag_id = task_instance.dag_id
            task_id = task_instance.task_id
        else:
            dag_id = data.get('dag_id', 'ecc_multi_table_import')
            task_id = data.get('task_id', 'unknown')

        if exception:
            error_message = str(exception)
            error_type = type(exception).__name__
        else:
            error_message = data.get('error_message', 'Erreur inconnue')
            error_type = data.get('error_type', 'UnknownError')

        execution_date = data.get('execution_date', datetime.now().isoformat())

        return {
            'title': 'Erreur Import ECC',
            'subtitle': execution_date,
            'dag_id': dag_id,
            'task_id': task_id,
            'error_message': error_message,
            'error_type': error_type,
            'execution_date': execution_date,
            'status': 'failed',
        }

    def _build_success_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template de succes ECC."""
        dag_id = data.get('dag_id', 'ecc_multi_table_import')
        execution_date = data.get('execution_date', datetime.now().isoformat())
        duration = data.get('duration', 'N/A')
        tables_imported = data.get('tables_imported', [])

        total_rows = sum(t.get('rows_inserted', t.get('rows', 0)) for t in tables_imported)
        total_updated = sum(t.get('rows_updated', 0) for t in tables_imported)
        total_fetched = sum(t.get('rows_fetched', t.get('rows_inserted', 0)) for t in tables_imported)
        total_skipped = sum(t.get('rows_skipped', 0) for t in tables_imported)

        return {
            'title': 'Import ECC Reussi',
            'subtitle': execution_date,
            'dag_id': dag_id,
            'execution_date': execution_date,
            'duration': duration,
            'tables_imported': tables_imported,
            'total_rows': total_rows,
            'total_updated': total_updated,
            'total_fetched': total_fetched,
            'total_skipped': total_skipped,
            'status': 'success',
        }

    def _build_error_subject(self, context: Dict[str, Any]) -> str:
        """Sujet : [ERREUR ECC] Import ECC - {dag_id} - {date}"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', 'ecc_multi_table_import')
        return f"[ERREUR ECC] Import ECC - {dag_id} - {date_str}"

    def _build_success_subject(self, context: Dict[str, Any]) -> str:
        """Sujet : [RAPPORT ECC] ecc_multi_table_import - {date}"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        return f"[RAPPORT ECC] ecc_multi_table_import - {date_str}"
