# amue/notifications/notifier.py
"""
Service de notification unifie pour les DAGs AMUE.

Ce module fournit un point d'entree unique pour toutes les notifications,
remplacant l'ancienne hierarchie de classes (BaseNotifier, ErrorNotifier, SuccessNotifier).
"""
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from amue.notifications.email_service import EmailService, Email
from amue.notifications.templates import NotificationTemplates
from amue.utils.config.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class NotificationType(Enum):
    """Types de notifications supportes"""
    SUCCESS = "success"
    ERROR = "error"


class NotificationService:
    """
    Service unifie de notification par email.

    Remplace l'ancienne architecture avec BaseNotifier/ErrorNotifier/SuccessNotifier.

    Usage:
        service = NotificationService()

        # Notification d'erreur
        service.notify_error({
            'dag_id': 'my_dag',
            'task_id': 'my_task',
            'error_message': 'Something went wrong',
            'error_type': 'AirflowException'
        })

        # Notification de succes
        service.notify_success({
            'dag_id': 'my_dag',
            'tables_imported': [...],
            'duration': '5m 30s'
        })
    """

    def __init__(self, email_service: Optional[EmailService] = None):
        """
        Initialise le service de notification.

        Args:
            email_service: Service d'envoi d'emails (cree si non fourni)
        """
        self.email_service = email_service or EmailService()
        self.recipients = self._load_recipients()

    def _load_recipients(self) -> List[str]:
        """Charge la liste des destinataires depuis les variables Airflow"""
        recipients_var = VarMgr.get('amue_report_recipients', default='admin@example.com')
        recipients = [r.strip() for r in recipients_var.split(',') if r.strip()]
        logger.debug(f"Destinataires charges: {recipients}")
        return recipients

    def notify_error(self, data: Dict[str, Any]) -> bool:
        """
        Envoie une notification d'erreur.

        Args:
            data: Donnees de l'erreur, peut contenir:
                - task_instance: Instance de task Airflow (optionnel)
                - exception: Exception survenue (optionnel)
                - dag_id: ID du DAG
                - task_id: ID de la tache
                - error_message: Message d'erreur
                - error_type: Type d'erreur

        Returns:
            True si l'envoi a reussi
        """
        logger.info("Envoi notification d'erreur")

        context = self._build_error_context(data)
        subject = self._build_error_subject(context)
        html_content = NotificationTemplates.render_error(context)

        email = Email(
            to=self.recipients,
            subject=subject,
            html_content=html_content
        )

        success = self.email_service.send(email)
        self._save_report(context, NotificationType.ERROR, success)

        return success

    def notify_success(self, data: Dict[str, Any]) -> bool:
        """
        Envoie une notification de succes.

        Args:
            data: Donnees de succes contenant:
                - dag_id: ID du DAG
                - execution_date: Date d'execution
                - duration: Duree d'execution
                - tables_imported: Liste des tables importees
                - total_rows: Nombre total de lignes inserees
                - total_fetched: Nombre total de lignes recuperees

        Returns:
            True si l'envoi a reussi
        """
        logger.info("Envoi notification de succes")

        context = self._build_success_context(data)
        subject = self._build_success_subject(context)
        html_content = NotificationTemplates.render_success(context)

        email = Email(
            to=self.recipients,
            subject=subject,
            html_content=html_content
        )

        success = self.email_service.send(email)
        self._save_report(context, NotificationType.SUCCESS, success)

        return success

    def _build_error_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template d'erreur"""
        # Gere le cas ou data vient d'un callback Airflow
        task_instance = data.get('task_instance')
        exception = data.get('exception')

        if task_instance:
            dag_id = task_instance.dag_id
            task_id = task_instance.task_id
        else:
            dag_id = data.get('dag_id', 'unknown')
            task_id = data.get('task_id', 'unknown')

        if exception:
            error_message = str(exception)
            error_type = type(exception).__name__
        else:
            error_message = data.get('error_message', 'Erreur inconnue')
            error_type = data.get('error_type', 'UnknownError')

        execution_date = data.get('execution_date', datetime.now().isoformat())

        return {
            'title': 'Erreur d\'Import AMUE',
            'subtitle': execution_date,
            'dag_id': dag_id,
            'task_id': task_id,
            'error_message': error_message,
            'error_type': error_type,
            'execution_date': execution_date,
            'status': 'failed'
        }

    def _build_success_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template de succes"""
        dag_id = data.get('dag_id', 'amue_multi_table_import')
        execution_date = data.get('execution_date', datetime.now().isoformat())
        duration = data.get('duration', 'N/A')
        tables_imported = data.get('tables_imported', [])

        # Calcule les totaux
        total_rows = sum(
            t.get('rows_inserted', t.get('rows', 0))
            for t in tables_imported
        )
        total_fetched = sum(
            t.get('rows_fetched', t.get('rows_inserted', 0))
            for t in tables_imported
        )

        return {
            'title': 'Import AMUE Reussi',
            'subtitle': execution_date,
            'dag_id': dag_id,
            'execution_date': execution_date,
            'duration': duration,
            'tables_imported': tables_imported,
            'total_rows': total_rows,
            'total_fetched': total_fetched,
            'status': 'success'
        }

    def _build_error_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email d'erreur"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', 'unknown')
        return f"[ERREUR] Import AMUE - {dag_id} - {date_str}"

    def _build_success_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email de succes"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        tables_count = len(context.get('tables_imported', []))
        total_rows = context.get('total_rows', 0)
        return f"[SUCCES] Import AMUE - {tables_count} table(s) - {total_rows:,} lignes - {date_str}"

    def _save_report(
        self,
        context: Dict[str, Any],
        notification_type: NotificationType,
        email_sent: bool
    ) -> None:
        """Sauvegarde le rapport dans les variables Airflow"""
        report = {
            'type': notification_type.value,
            'timestamp': datetime.now().isoformat(),
            'email_sent': email_sent,
            'recipients': self.recipients,
            **context
        }

        try:
            VarMgr.set('last_import_report', json.dumps(report, default=str))
            logger.debug("Rapport sauvegarde")
        except Exception as e:
            logger.warning(f"Echec sauvegarde rapport: {e}")
