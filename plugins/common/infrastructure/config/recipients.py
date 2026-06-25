"""Layer: infrastructure

Parsing des listes de destinataires email configurées via une variable Airflow
(chaîne CSV) — logique identique répétée dans amue/ecc `settings.py`.
"""
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


def parse_recipients(raw: Optional[str], *, warning_message: Optional[str] = None) -> List[str]:
    """Parse une chaîne CSV de destinataires en liste nettoyée.

    Args:
        raw: Chaîne CSV brute (ex: "a@x.com, b@x.com"), ou None/vide.
        warning_message: Message loggué en warning si la liste résultante est vide.

    Returns:
        Liste des adresses non vides, avec espaces retirés.
    """
    recipients = [r.strip() for r in (raw or '').split(',') if r.strip()]
    if not recipients and warning_message:
        logger.warning(warning_message)
    return recipients
