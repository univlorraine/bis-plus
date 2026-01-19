# amue/utils/airflow_helpers.py
import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AirflowVariableManager:
    """Gestionnaire centralisé pour variables Airflow avec fallback SDK/API"""

    @staticmethod
    def set(key: str, value: Any, description: Optional[str] = None) -> bool:
        """
        Définit une variable Airflow avec fallback automatique

        Returns:
            True si succès, False sinon
        """
        # Sérialise si nécessaire
        if not isinstance(value, str):
            value = json.dumps(value)

        try:
            from airflow.sdk import Variable
            Variable.set(key, value, description)
            logger.info(f"[VAR] Set '{key}'")
            return True
        except (ImportError, AttributeError, Exception) as e:
            logger.error(f"[ERROR] Cannot set variable '{key}': {e}")
            return False

    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """Récupère une variable avec fallback"""
        try:
            from airflow.sdk import Variable
            return Variable.get(key, default=default)
        except (ImportError, AttributeError, KeyError):
            try:
                from airflow.models import Variable
                return Variable.get(key, default_var=default)
            except (ImportError, AttributeError, KeyError):
                return default
