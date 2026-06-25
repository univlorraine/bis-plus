"""Task de déclenchement conditionnel de amue_table_setup après découverte."""
import logging
from typing import List

from airflow.sdk import task

logger = logging.getLogger(__name__)


@task(task_id='trigger_setup_if_needed')
def trigger_setup_if_needed(added: List[str], **context) -> bool:
    """
    Indique si amue_table_setup doit être déclenché.
    Le déclenchement effectif est réalisé par TriggerDagRunOperator dans le DAG.

    Params Airflow consommés (context['params']) :
        trigger_setup : si False, désactive le déclenchement automatique

    Returns:
        True si amue_table_setup doit être déclenché, False sinon
    """
    params = context['params']
    should_trigger = bool(added) and params.get('trigger_setup', True)
    if not should_trigger:
        logger.info(
            "[DISCOVERY] amue_table_setup non déclenché "
            "(aucune table ajoutée ou option désactivée)"
        )
    else:
        logger.info(
            f"[DISCOVERY] {len(added)} nouvelle(s) table(s) — déclenchement de amue_table_setup"
        )
    return should_trigger
