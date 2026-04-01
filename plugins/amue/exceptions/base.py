"""Exception de base pour toutes les erreurs AMUE."""
from typing import Optional, Any, Dict
from datetime import datetime


class AMUEError(Exception):
    """
    Exception de base pour toutes les erreurs AMUE.

    Attributes:
        message: Message d'erreur
        timestamp: Date/heure de l'erreur
        correlation_id: ID de corrélation pour le tracing
        context: Contexte additionnel (dict)
    """

    def __init__(
        self,
        message: str,
        correlation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.timestamp = datetime.now()
        self.correlation_id = correlation_id
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        base = self.message
        if self.correlation_id:
            base = f"[{self.correlation_id}] {base}"
        return base

    def to_dict(self) -> Dict[str, Any]:
        """Convertit l'exception en dictionnaire pour le logging structuré"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id,
            'context': self.context
        }
