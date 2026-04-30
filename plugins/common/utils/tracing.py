"""Helpers de tracing partagés AMUE / ECC."""
from typing import Optional


def to_iso_str(v) -> Optional[str]:
    """
    Convertit une valeur datetime-like en chaîne ISO 8601.

    Args:
        v: Valeur à convertir (datetime, date, ou déjà une chaîne)

    Returns:
        Chaîne ISO 8601 ou None si v est None
    """
    if v is None:
        return None
    return v.isoformat() if hasattr(v, 'isoformat') else str(v)
