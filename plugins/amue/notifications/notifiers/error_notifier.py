# amue/notifications/notifiers/error_notifier.py
"""Notifier pour les erreurs"""
from datetime import datetime
from typing import Dict, Any

from amue.notifications.notifiers.base import BaseNotifier
from amue.notifications.templates.base import BaseTemplate
from amue.notifications.templates.error import ErrorTemplate


class ErrorNotifier(BaseNotifier):
    """
    Notifier pour les erreurs d'import

    Usage:
        notifier = ErrorNotifier()
        notifier.notify({
            'dag_id': 'my_dag',
            'task_id': 'my_task',
            'error_message': 'Something went wrong',
            'error_type': 'AirflowException'
        })
    """

    @property
    def template(self) -> BaseTemplate:
        return ErrorTemplate()

    def build_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construit le contexte pour le template d'erreur

        Args:
            data: Données de l'erreur (peut venir du callback Airflow)

        Returns:
            Contexte formaté
        """
        # Gère le cas où data vient d'un callback Airflow
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

    def build_subject(self, context: Dict[str, Any]) -> str:
        """Construit le sujet de l'email d'erreur"""
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', 'unknown')
        return f"[ERREUR] Import AMUE - {dag_id} - {date_str}"


# ============================================================================
# FONCTION CALLBACK POUR AIRFLOW
# ============================================================================

def send_failure_notification(context: Dict[str, Any]) -> None:
    """
    Callback Airflow pour envoyer une notification en cas d'échec

    Cette fonction est appelée automatiquement par Airflow quand:
    - Une tâche échoue (task-level callback)
    - Un DAG échoue (dag-level callback)

    Args:
        context: Contexte Airflow avec task_instance, exception, etc.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info("Déclenchement du callback d'erreur")

    # Vérifie qu'il y a bien une exception avant d'envoyer
    exception = context.get('exception')
    if not exception:
        logger.info("Pas d'exception dans le contexte - notification ignorée")
        return

    try:
        notifier = ErrorNotifier()
        success = notifier.notify(context)

        if success:
            logger.info("Notification d'erreur envoyée avec succès")
        else:
            logger.warning("Échec de l'envoi de la notification d'erreur")

    except Exception as e:
        logger.error(f"Erreur dans le callback de notification: {e}")

    logger.info("Callback d'erreur terminé")
