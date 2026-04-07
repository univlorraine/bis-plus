# amue/notifications/templates.py
"""
Templates HTML unifiés pour les notifications AMUE.

Ce module assemble tous les templates par type de DAG en une seule classe
NotificationTemplates pour la compatibilité avec les services existants.

Structure des templates :
    templates_base.py     — styles CSS et helpers HTML partagés
    templates_error.py    — render_error         (tous les DAGs)
    templates_import.py   — render_success        (import AMUE / ECC)
    templates_sync.py     — render_sync_success   (amue_sync_schemas)
    templates_rollback.py — render_rollback_success (amue_rollback)
    templates_setup.py    — render_setup_error    (amue_table_setup)
"""
from amue.notifications.templates_base import BaseTemplates
from amue.notifications.templates_error import ErrorTemplates
from amue.notifications.templates_import import ImportTemplates
from amue.notifications.templates_refresh_views import RefreshViewsTemplates
from amue.notifications.templates_rollback import RollbackTemplates
from amue.notifications.templates_setup import SetupTemplates
from amue.notifications.templates_sync import SyncTemplates


class NotificationTemplates(
    ErrorTemplates,
    ImportTemplates,
    SyncTemplates,
    RollbackTemplates,
    SetupTemplates,
    RefreshViewsTemplates,
    BaseTemplates,
):
    """
    Classe assemblant tous les templates de notification.

    Méthodes disponibles :
        render_error(context)                  — erreur générique
        render_success(context)                — import AMUE / ECC réussi
        render_sync_success(context)           — synchronisation blue/green réussie
        render_rollback_success(context)       — rollback blue/green réussi
        render_setup_error(context)            — anomalie de setup (tables bloquées)
        render_refresh_views_success(context)  — rafraîchissement vues custom réussi
    """
