# amue/utils/airflow_helpers.py
import json
from amue.utils.logger import get_logger

logger = get_logger(__name__)


class AirflowVariableManager:
    """Gestionnaire centralisé pour variables Airflow avec fallback SDK/API"""

    @staticmethod
    def set(key: str, value: any) -> bool:
        """
        Définit une variable Airflow avec fallback automatique

        Returns:
            True si succès, False sinon
        """
        # Sérialise si nécessaire
        if not isinstance(value, str):
            value = json.dumps(value)

        # Fallback SDK
        try:
            from airflow.sdk import Variable
            Variable.set(key, value)
            logger.info(f"[VAR] Set '{key}' via SDK")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Cannot set variable '{key}': {e}")
            return False

    @staticmethod
    def get(key: str, default: any = None) -> any:
        """Récupère une variable avec fallback"""
        try:
            from airflow.sdk import Variable
            return Variable.get(key, default=default)
        except:
            try:
                from airflow.models import Variable
                return Variable.get(key, default_var=default)
            except:
                return default