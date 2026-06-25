# ecc/utils/config/settings.py
"""Layer: infrastructure

Configuration centralisée ECC."""
import logging

from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr
from common.infrastructure.config.recipients import parse_recipients
from common.domain.protected_source import PROTECTED_SOURCE

logger = logging.getLogger(__name__)


class ECCDefaults:
    """
    Constantes par défaut pour le module ECC.

    Utilisation:
        from ecc.infrastructure.config.settings import ECCDefaults
        batch_size = ECCDefaults.IMPORT_BATCH_SIZE
    """

    IMPORT_SCHEDULE: str = '0 4 * * *'
    IMPORT_BATCH_SIZE: int = 5000
    SOURCE_NAME: str = 'ecc'
    PROTECTED_SOURCE: str = PROTECTED_SOURCE  # centralisé dans common.domain.protected_source
    ECC_CONN_ID: str = 'ecc_data'
    SQL_DIR: str = '/opt/airflow/scripts/sql/ECC'


def get_ecc_batch_size() -> int:
    """Récupère la taille de batch ECC depuis Airflow (défaut: 5000)."""
    return VarMgr.get_int('ecc_import_batch_size', ECCDefaults.IMPORT_BATCH_SIZE, min_value=1)


def get_ecc_recipients() -> list:
    """Récupère les destinataires des rapports ECC depuis Airflow."""
    return parse_recipients(
        VarMgr.get('ecc_report_recipients', default=None),
        warning_message=(
            "Variable Airflow 'ecc_report_recipients' vide ou non configurée"
            " — les notifications email ECC ne seront pas envoyées"
        ),
    )
