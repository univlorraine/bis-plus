# amue/utils/logger.py
"""
Logger unifié pour AMUE
Remplace les print() dispersés par un logging Python standard
"""
import logging
from typing import Optional


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """
    Retourne un logger configuré pour AMUE

    Args:
        name: Nom du module (__name__)
        level: Niveau de log (default: INFO)

    Returns:
        Logger configuré
    """
    logger = logging.getLogger(f"amue.{name}")

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level or logging.INFO)
    return logger
