"""
Services Blue/Green - Gestion du déploiement blue/green

Modules :
    - bluegreen_manager         : Façade orchestrant les composants blue/green
    - bluegreen_schema_resolver : Résolution des noms de schémas
    - bluegreen_state_manager   : État et persistance en BDD
    - bluegreen_lock_manager    : Verrou exclusif d'import
    - view_switcher             : Switch atomique des vues
    - schema_synchronizer       : Synchronisation des schémas
"""
from common.application.bluegreen.bluegreen_manager import BlueGreenManager, BlueGreenState
from common.application.bluegreen.bluegreen_schema_resolver import BlueGreenSchemaResolver
from common.application.bluegreen.bluegreen_state_manager import BlueGreenStateManager
from common.application.bluegreen.bluegreen_lock_manager import BlueGreenLockManager

__all__ = [
    'BlueGreenManager',
    'BlueGreenState',
    'BlueGreenSchemaResolver',
    'BlueGreenStateManager',
    'BlueGreenLockManager',
]
