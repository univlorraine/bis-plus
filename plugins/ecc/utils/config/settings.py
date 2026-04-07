# ecc/utils/config/settings.py
"""Configuration centralisée ECC."""
import logging

from common.utils.config.airflow_helpers import AirflowVariableManager as VarMgr
from common.config import PROTECTED_SOURCE

logger = logging.getLogger(__name__)


class ECCDefaults:
    """
    Constantes par défaut pour le module ECC.

    Utilisation:
        from ecc.utils.config.settings import ECCDefaults
        batch_size = ECCDefaults.IMPORT_BATCH_SIZE
    """

    IMPORT_SCHEDULE: str = '0 4 * * *'
    IMPORT_BATCH_SIZE: int = 5000
    SOURCE_NAME: str = 'ecc'
    PROTECTED_SOURCE: str = PROTECTED_SOURCE  # centralisé dans common.config
    ORACLE_CONN_ID: str = 'oracle_data'
    SQL_DIR: str = '/opt/airflow/scripts/sql/ECC'


def get_ecc_batch_size() -> int:
    """Récupère la taille de batch ECC depuis Airflow (défaut: 5000)."""
    return int(VarMgr.get('ecc_import_batch_size', ECCDefaults.IMPORT_BATCH_SIZE))


def get_ecc_recipients() -> list:
    """Récupère les destinataires des rapports ECC depuis Airflow."""
    recipients_str = VarMgr.get('ecc_report_recipients', default=None)
    if not recipients_str:
        logger.warning(
            "Variable Airflow 'ecc_report_recipients' non configurée"
            " — les notifications email ECC ne seront pas envoyées"
        )
        return []
    return [r.strip() for r in recipients_str.split(',') if r.strip()]
