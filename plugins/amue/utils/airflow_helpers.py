"""
Helpers pour l'interaction avec les variables Airflow.

================================================================================
RÔLE DU MODULE
================================================================================

Ce module fournit une interface unifiée pour lire/écrire les variables Airflow,
avec compatibilité automatique entre différentes versions d'Airflow.

POURQUOI CE WRAPPER ?
    Airflow 2.x utilise : from airflow.models import Variable
    Airflow 3.x utilise : from airflow.sdk import Variable

    Ce wrapper détecte automatiquement la version et utilise l'import correct.

================================================================================
USAGE
================================================================================

    from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

    # Lecture
    value = VarMgr.get('my_variable', default='fallback')

    # Écriture
    success = VarMgr.set('my_variable', 'new_value')

    # Écriture d'un objet (sérialisé en JSON)
    success = VarMgr.set('config', {'key': 'value'})

================================================================================
SÉRIALISATION
================================================================================

La méthode set() accepte n'importe quel type de valeur :
    - str : stocké tel quel
    - dict/list/int/etc : sérialisé en JSON automatiquement

La méthode get() retourne toujours une string. Pour les objets JSON,
le parsing doit être fait manuellement :
    config = json.loads(VarMgr.get('config'))

================================================================================
"""
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

        logger.info(f"[VAR_SET] Tentative set '{key}' ({len(value)} chars)")

        # Essai Airflow 3.x SDK - mais attention, Variable.set() peut ne pas exister
        try:
            from airflow.sdk import Variable
            if hasattr(Variable, 'set'):
                Variable.set(key, value, description)
                logger.info(f"[VAR_SET] SUCCESS via SDK pour '{key}'")
                return True
            else:
                logger.info(f"[VAR_SET] SDK Variable n'a pas de méthode set(), fallback models")
        except ImportError:
            logger.info(f"[VAR_SET] airflow.sdk non disponible, fallback models")
        except Exception as e:
            logger.error(f"[VAR_SET] SDK Variable.set failed for '{key}': {type(e).__name__}: {e}")
            # Ne pas retourner False ici, essayer le fallback

        # Fallback Airflow 2.x / 3.x models
        try:
            from airflow.models import Variable
            Variable.set(key, value, description=description)
            logger.info(f"[VAR_SET] SUCCESS via models pour '{key}'")
            return True
        except ImportError:
            logger.error(f"[VAR_SET] airflow.models non disponible non plus!")
            return False
        except Exception as e:
            logger.error(f"[VAR_SET] Models Variable.set failed for '{key}': {type(e).__name__}: {e}")
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
