"""Intégration structlog optionnelle pour le tracing de corrélation."""
from __future__ import annotations


def is_enabled() -> bool:
    """Retourne True si structlog est disponible et peut binder des contextvars."""
    try:
        import structlog  # noqa: F401
        return True
    except ImportError:
        return False
