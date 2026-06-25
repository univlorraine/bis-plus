# ecc/notifications/ecc_notifier.py
"""Layer: infrastructure

Service de notification pour le DAG ECC."""
import logging
from datetime import datetime
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from common.infrastructure.notifications.base_notifier import BaseNotificationService
from ecc.infrastructure.notifications.ecc_templates import ECCNotificationTemplates
from ecc.infrastructure.config.settings import get_ecc_recipients

logger = logging.getLogger(__name__)
_TZ_PARIS = ZoneInfo('Europe/Paris')

class ECCNotificationService(BaseNotificationService):
    """Service de notification par email pour le DAG ECC."""

    SYSTEM_NAME = 'ECC'
    DEFAULT_DAG_ID = 'ecc_multi_table_import'
    TEMPLATES_CLASS = ECCNotificationTemplates

    def _load_recipients(self) -> List[str]:
        """Charge les destinataires depuis la variable Airflow ecc_report_recipients."""
        recipients = get_ecc_recipients()
        logger.debug(f"Destinataires ECC chargés: {recipients}")
        return recipients

    def _build_error_subject(self, context: Dict[str, Any]) -> str:
        """Sujet : [ERREUR ECC] Import ECC - {dag_id} - {date}"""
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', self.DEFAULT_DAG_ID)
        return f"[ERREUR ECC] Import ECC - {dag_id} - {date_str}"

    def _build_success_subject(self, context: Dict[str, Any]) -> str:
        """Sujet : [RAPPORT ECC] {dag_id} - {date}"""
        date_str = datetime.now(tz=_TZ_PARIS).strftime('%Y-%m-%d %H:%M')
        dag_id = context.get('dag_id', self.DEFAULT_DAG_ID)
        return f"[RAPPORT ECC] {dag_id} - {date_str}"

    def _extra_success_fields(self, data: Dict[str, Any], tables: list) -> Dict[str, Any]:
        """Ajoute le total des lignes ignorées au contexte de succes."""
        return {'total_skipped': sum(t.get('rows_skipped', 0) for t in tables)}
