# common/notifications/base_notifier.py
"""Classe de base commune pour les services de notification AMUE et ECC."""
import logging
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

_TZ_PARIS = ZoneInfo('Europe/Paris')

from common.notifications.email_service import Email, EmailService

logger = logging.getLogger(__name__)


class BaseNotificationService:
    """
    Classe de base pour les services de notification par email.

    Sous-classes requises :
        - SYSTEM_NAME     : nom du système ('AMUE' ou 'ECC')
        - DEFAULT_DAG_ID  : ID du DAG par défaut
        - TEMPLATES_CLASS : classe de templates à utiliser

    Méthodes abstraites à implémenter :
        - _load_recipients()
        - _build_error_subject(context)
        - _build_success_subject(context)
    """

    SYSTEM_NAME: str = ''
    DEFAULT_DAG_ID: str = ''
    TEMPLATES_CLASS = None

    def __init__(self, email_service: Optional[EmailService] = None):
        self.email_service = email_service or EmailService()
        self.recipients = self._load_recipients()

    def _load_recipients(self) -> List[str]:
        raise NotImplementedError

    def _build_error_subject(self, context: Dict[str, Any]) -> str:
        raise NotImplementedError

    def _build_success_subject(self, context: Dict[str, Any]) -> str:
        raise NotImplementedError

    def notify_error(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification d'erreur."""
        if not self.recipients:
            logger.warning(f"[{self.SYSTEM_NAME}] Aucun destinataire configuré — notification ignorée")
            return False
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification d'erreur")

        context = self._build_error_context(data)
        subject = self._build_error_subject(context)
        html_content = self.TEMPLATES_CLASS.render_error(context)

        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def notify_success(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification de succès."""
        if not self.recipients:
            logger.warning(f"[{self.SYSTEM_NAME}] Aucun destinataire configuré — notification ignorée")
            return False
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification de succès")

        context = self._build_success_context(data)
        subject = self._build_success_subject(context)
        html_content = self.TEMPLATES_CLASS.render_success(context)

        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def _build_error_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template d'erreur."""
        task_instance = data.get('task_instance')
        exception = data.get('exception')
        dag_run = data.get('dag_run')

        if task_instance:
            dag_id = task_instance.dag_id
            task_id = task_instance.task_id
        else:
            dag_id = data.get('dag_id') or (dag_run.dag_id if dag_run else None) or self.DEFAULT_DAG_ID
            task_id = data.get('task_id', 'unknown')

        if exception:
            error_message = str(exception)
            error_type = type(exception).__name__
            error_traceback = ''.join(
                traceback.format_exception(type(exception), exception, exception.__traceback__)
            )
        else:
            error_message = data.get('error_message', 'Erreur inconnue')
            error_type = data.get('error_type', 'UnknownError')
            error_traceback = None

        execution_date = self._format_date(data.get('execution_date'))

        return {
            'title': f"Erreur Import {self.SYSTEM_NAME}",
            'subtitle': f"{dag_id} · {execution_date}",
            'dag_id': dag_id,
            'task_id': task_id,
            'error_message': error_message,
            'error_type': error_type,
            'error_traceback': error_traceback,
            'execution_date': execution_date,
            'status': 'failed',
            'failed_tasks': data.get('failed_tasks', []),
        }

    def _build_success_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Construit le contexte pour le template de succes."""
        dag_id = data.get('dag_id', self.DEFAULT_DAG_ID)
        execution_date = self._format_date(data.get('execution_date'))
        duration = data.get('duration', 'N/A')
        tables_imported = data.get('tables_imported', [])

        total_rows = sum(t.get('rows_inserted', t.get('rows', 0)) for t in tables_imported)
        total_updated = sum(t.get('rows_updated', 0) for t in tables_imported)
        total_fetched = sum(t.get('rows_fetched', t.get('rows_inserted', 0)) for t in tables_imported)

        context = {
            'title': data.get('title') or f"Import {self.SYSTEM_NAME} Réussi",
            'subtitle': execution_date,
            'dag_id': dag_id,
            'execution_date': execution_date,
            'duration': duration,
            'tables_imported': tables_imported,
            'total_rows': total_rows,
            'total_updated': total_updated,
            'total_fetched': total_fetched,
            'status': 'success',
        }
        context.update(self._extra_success_fields(data, tables_imported))
        return context

    def _extra_success_fields(self, data: Dict[str, Any], tables: list) -> Dict[str, Any]:
        """Champs supplémentaires pour le contexte de succes. Surcharger si besoin."""
        return {}

    @staticmethod
    def _format_date(value: Any) -> str:
        """Formate une date en chaîne lisible 'YYYY-MM-DD HH:MM' en heure de Paris.

        Accepte un objet datetime (naïf ou avec timezone), une chaîne ISO,
        ou None (retourne la date/heure courante à Paris).
        """
        fmt = '%Y-%m-%d %H:%M'
        if value is None:
            return datetime.now(tz=_TZ_PARIS).strftime(fmt)
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(_TZ_PARIS)
            return value.strftime(fmt)
        # Chaîne ISO : on parse puis on convertit
        raw = str(value)
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is not None:
                dt = dt.astimezone(_TZ_PARIS)
            return dt.strftime(fmt)
        except ValueError:
            pass
        # Fallback : retourner la valeur brute telle quelle
        return raw

    # ------------------------------------------------------------------
    # Notifications spécialisées (sync, rollback, setup)
    # ------------------------------------------------------------------

    def notify_sync_success(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification de succès de synchronisation blue/green."""
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification de synchronisation")

        tables_detail = data.get('tables_imported', [])
        tables_synced = len(tables_detail)
        tables_failed = data.get('tables_failed', 0)
        source = data.get('sync_source', data.get('source', '?'))
        target = data.get('sync_target', data.get('target', '?'))
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')

        context = {
            'title': data.get('title', f"Synchronisation {self.SYSTEM_NAME} Réussie"),
            'subtitle': self._format_date(data.get('execution_date')),
            'dag_id': data.get('dag_id', self.DEFAULT_DAG_ID),
            'source': source,
            'target': target,
            'tables_synced': tables_synced,
            'tables_failed': tables_failed,
            'tables_detail': tables_detail,
        }
        suffix = ' (partiel)' if tables_failed > 0 else ''
        subject = (
            f"[SUCCÈS] Synchronisation {self.SYSTEM_NAME}"
            f" — {tables_synced} table(s){suffix} — {date_str}"
        )
        html_content = self.TEMPLATES_CLASS.render_sync_success(context)
        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def notify_rollback_success(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification de succès de rollback blue/green."""
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification de rollback")

        previous = data.get('previous_active', '?')
        new_active = data.get('new_active', '?')
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')

        context = {
            'title': data.get('title', f"Rollback {self.SYSTEM_NAME} Réussi"),
            'subtitle': date_str,
            'dag_id': data.get('dag_id', self.DEFAULT_DAG_ID),
            'previous_schema': previous,
            'new_schema': new_active,
        }
        subject = (
            f"[SUCCÈS] Rollback {self.SYSTEM_NAME}"
            f" — {previous} → {new_active} — {date_str}"
        )
        html_content = self.TEMPLATES_CLASS.render_rollback_success(context)
        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def notify_refresh_views_success(self, data: Dict[str, Any]) -> bool:
        """Envoie une notification de succès de rafraîchissement des vues custom."""
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification de rafraîchissement des vues")

        ok = data.get('ok', 0)
        ko = data.get('ko', 0)
        target_schema = data.get('target_schema', '?')
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')

        ko_label = f" — {ko} erreur(s)" if ko > 0 else ''
        context = {
            'title': data.get('title', f"Rafraîchissement Vues {self.SYSTEM_NAME}{ko_label}"),
            'subtitle': self._format_date(data.get('execution_date')),
            'dag_id': data.get('dag_id', self.DEFAULT_DAG_ID),
            'target_schema': target_schema,
            'ok': ok,
            'ko': ko,
            'files_processed': data.get('files_processed', []),
            'files_failed': data.get('files_failed', []),
        }
        suffix = ' (partiel)' if ko > 0 else ''
        subject = (
            f"[SUCCÈS] Rafraîchissement vues {self.SYSTEM_NAME}"
            f" — {ok} vue(s){suffix} → {target_schema} — {date_str}"
        )
        html_content = self.TEMPLATES_CLASS.render_refresh_views_success(context)
        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)

    def notify_setup_error(self, data: Dict[str, Any]) -> bool:
        """Envoie une alerte d'anomalie de setup (tables bloquées ou en erreur)."""
        logger.info(f"[{self.SYSTEM_NAME}] Envoi notification de setup")

        tables_blocked = data.get('tables_blocked', [])
        tables_error = data.get('tables_error', [])
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')

        context = {
            'title': data.get('title', f"Anomalie Setup {self.SYSTEM_NAME}"),
            'subtitle': date_str,
            'dag_id': data.get('dag_id', self.DEFAULT_DAG_ID),
            'tables_blocked': tables_blocked,
            'tables_error': tables_error,
        }
        subject = (
            f"[ALERTE] Setup {self.SYSTEM_NAME}"
            f" — {len(tables_blocked)} bloquée(s)"
            f", {len(tables_error)} erreur(s) — {date_str}"
        )
        html_content = self.TEMPLATES_CLASS.render_setup_error(context)
        email = Email(to=self.recipients, subject=subject, html_content=html_content)
        return self.email_service.send(email)
