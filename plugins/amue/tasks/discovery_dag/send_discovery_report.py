"""Task de rapport de la découverte de tables AMUE."""
import logging
from typing import Dict, List

from airflow.sdk import task

logger = logging.getLogger(__name__)


@task(task_id='send_discovery_report')
def send_discovery_report(discovery: Dict[str, List[str]], added: List[str], setup_triggered: bool) -> Dict:
    """
    Log un résumé de la découverte/enregistrement de tables.

    Args:
        discovery: Résultat de discover_tables()
        added: Tables effectivement enregistrées par register_tables()
        setup_triggered: Résultat de trigger_setup_if_needed()

    Returns:
        Résumé : {available, new, added, setup_triggered}
    """
    available = discovery.get('available', [])
    new = discovery.get('new', [])
    not_added = [t for t in new if t not in added]

    logger.info("[DISCOVERY_REPORT] Résumé de la découverte AMUE:")
    logger.info(f"  - Tables exposées par l'API : {len(available)}")
    logger.info(f"  - Nouvelles (non enregistrées avant ce run) : {len(new)} {new}")
    logger.info(f"  - Enregistrées dans ce run : {len(added)} {added}")
    if not_added:
        logger.info(f"  - Toujours en attente de sélection : {len(not_added)} {not_added}")
    logger.info(f"  - amue_table_setup déclenché : {setup_triggered}")

    return {
        'available': len(available),
        'new': len(new),
        'added': len(added),
        'setup_triggered': setup_triggered,
    }
