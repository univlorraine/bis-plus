# amue/notifications/templates_error.py
"""
Templates d'email pour les notifications d'erreur.

render_error() est le point d'entrée unique. Il dispatche vers un sous-template
adapté au type d'erreur, avec fallback générique si aucun ne correspond.

Sous-templates disponibles :
    DAGFailure                                          → _render_error_dag_failure
    AMUENetworkError / ConnectionError / TimeoutError … → _render_error_network
    AMUEAuthError                                       → _render_error_auth
    ConcurrentImportError                               → _render_error_concurrent
    StructureChangeDetected / AMUEStructureChangedError → _render_error_structure
    TableNotFoundError / AMUETableNotFoundError         → _render_error_table_not_found
    SyncError                                           → _render_error_sync
    ViewSwitchError / AMUEBlueGreenError                → _render_error_view_switch
    AMUEDatabaseError / AMUEBatchError / AMUEImportError → _render_error_database
    Fallback                                            → _render_error_default (extrait tronqué)
"""
from typing import Any, Dict, List

from common.notifications.base_templates import BaseTemplates

# --- Groupes d'error_type pour le dispatch ---

_NETWORK_ERRORS = frozenset({
    'AMUENetworkError',
    'AMUEAPIError',
    'ConnectionError',
    'ConnectionResetError',
    'TimeoutError',
    'OSError',
})

_AUTH_ERRORS = frozenset({
    'AMUEAuthError',
})

_CONCURRENT_ERRORS = frozenset({
    'ConcurrentImportError',
})

_STRUCTURE_ERRORS = frozenset({
    'StructureChangeDetected',
    'AMUEStructureChangedError',
    'AMUESchemaError',
})

_TABLE_NOT_FOUND_ERRORS = frozenset({
    'TableNotFoundError',
    'AMUETableNotFoundError',
})

_SYNC_ERRORS = frozenset({
    'SyncError',
})

_VIEW_SWITCH_ERRORS = frozenset({
    'ViewSwitchError',
    'AMUEBlueGreenError',
})

_DATABASE_ERRORS = frozenset({
    'AMUEDatabaseError',
    'AMUEBatchError',
    'AMUEImportError',
})

_ERROR_TITLES = {
    'DAGFailure':                'Échec DAG — Import AMUE',
    'AMUENetworkError':          'Erreur Réseau — Import AMUE',
    'AMUEAPIError':              'Erreur API — Import AMUE',
    'ConnectionError':           'Erreur Réseau — Import AMUE',
    'ConnectionResetError':      'Erreur Réseau — Import AMUE',
    'TimeoutError':              'Timeout Réseau — Import AMUE',
    'OSError':                   'Erreur Réseau — Import AMUE',
    'AMUEAuthError':             'Erreur Authentification — Import AMUE',
    'ConcurrentImportError':     'Import Concurrent Détecté',
    'StructureChangeDetected':   'Structure Modifiée — Import AMUE',
    'AMUEStructureChangedError': 'Structure Modifiée — Import AMUE',
    'AMUESchemaError':           'Erreur Schéma — Import AMUE',
    'TableNotFoundError':        'Table Introuvable — Import AMUE',
    'AMUETableNotFoundError':    'Table Introuvable — Import AMUE',
    'SyncError':                 'Erreur Sync Blue/Green — AMUE',
    'ViewSwitchError':           'CRITIQUE — Switch Vues AMUE',
    'AMUEBlueGreenError':        'CRITIQUE — Blue/Green AMUE',
    'AMUEDatabaseError':         'Erreur Base de Données — Import AMUE',
    'AMUEBatchError':            'Erreur Batch Insert — Import AMUE',
    'AMUEImportError':           'Erreur Import — Import AMUE',
}


class ErrorTemplates(BaseTemplates):
    """Templates HTML pour les notifications d'erreur, avec dispatch par type."""

    # ------------------------------------------------------------------
    # Point d'entrée public
    # ------------------------------------------------------------------

    @classmethod
    def render_error(cls, context: Dict[str, Any]) -> str:
        """
        Génère le HTML pour une notification d'erreur.

        Dispatche vers un sous-template spécialisé selon error_type,
        ou vers le template générique (extrait) si aucun ne correspond.

        Contexte attendu :
            title         : titre de l'email
            subtitle      : date
            dag_id        : ID du DAG
            task_id       : ID de la tâche
            error_type    : type d'erreur (nom de classe ou chaîne nommée)
            error_message : message d'erreur complet
            status        : statut (failed)
            failed_tasks  : liste de dicts {task_id, map_index, duration}
        """
        error_type = context.get('error_type', 'UnknownError')
        context['title'] = _ERROR_TITLES.get(error_type, f'Erreur {error_type} — Import AMUE')

        if error_type == 'DAGFailure':
            return cls._render_error_dag_failure(context)
        if error_type in _NETWORK_ERRORS:
            return cls._render_error_network(context)
        if error_type in _AUTH_ERRORS:
            return cls._render_error_auth(context)
        if error_type in _CONCURRENT_ERRORS:
            return cls._render_error_concurrent(context)
        if error_type in _STRUCTURE_ERRORS:
            return cls._render_error_structure(context)
        if error_type in _TABLE_NOT_FOUND_ERRORS:
            return cls._render_error_table_not_found(context)
        if error_type in _SYNC_ERRORS:
            return cls._render_error_sync(context)
        if error_type in _VIEW_SWITCH_ERRORS:
            return cls._render_error_view_switch(context)
        if error_type in _DATABASE_ERRORS:
            return cls._render_error_database(context)
        return cls._render_error_default(context)

    # ------------------------------------------------------------------
    # Sous-templates spécialisés
    # ------------------------------------------------------------------

    @classmethod
    def _render_error_dag_failure(cls, context: Dict[str, Any]) -> str:
        """
        Template pour un échec au niveau DAG (on_failure_callback DAG-level).

        Affiche la liste des tâches en échec de manière prominente.
        Pas de section "message d'erreur" car l'exception n'est pas remontée.
        """
        title = context.get('title', 'Échec du DAG')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        failed_tasks = context.get('failed_tasks', [])
        error_message = context.get('error_message', '')

        summary_html = ''
        if error_message:
            summary_html = f"""
            <div style="background: #f8f9fa; padding: 12px 15px; border-radius: 4px;
                        margin-top: 15px; font-size: 14px; color: #555;">
                {cls.escape_html(error_message)}
            </div>"""

        traceback_html = cls._render_stacktrace(context.get('error_traceback'))
        tasks_html = cls._render_failed_tasks(failed_tasks) if failed_tasks else ''

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Le DAG a échoué
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Une ou plusieurs tâches sont en état <code>failed</code>.
                Consultez les tâches ci-dessous et les logs Airflow pour le détail.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâches en échec :</div>
                <div class="info-value">
                    <strong>{len(failed_tasks)}</strong> tâche(s)
                </div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>
            {summary_html}
        </div>
        {traceback_html}
        {tasks_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(show_actions=True)
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_network(cls, context: Dict[str, Any]) -> str:
        """
        Template pour les erreurs réseau (timeout, connexion refusée, API indisponible).
        """
        title = context.get('title', 'Erreur Réseau')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'NetworkError')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur de Connexion Réseau
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Le DAG n'a pas pu joindre la source de données ou la base de données PostgreSQL.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Extrait de l'erreur :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez que le VPN est actif et que la source de données est joignable<br>"
                "- Vérifiez la connexion PostgreSQL (host, port, credentials)<br>"
                "- Consultez les logs Airflow pour l'URL et le code d'erreur précis<br>"
                "- Relancez le DAG une fois la connectivité rétablie"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_auth(cls, context: Dict[str, Any]) -> str:
        """
        Template pour les erreurs d'authentification OAuth.
        """
        title = context.get('title', "Erreur d'Authentification")
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur d'Authentification OAuth
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Le token OAuth est expiré, invalide, ou les credentials sont incorrects.
                L'accès à l'API AMUE a été refusé (HTTP 401).
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>AMUEAuthError</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Extrait de l'erreur :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez les variables Airflow : <code>amue_client_id</code>,"
                " <code>amue_client_secret</code>, <code>amue_token_url</code><br>"
                "- Vérifiez que le compte de service n'est pas bloqué<br>"
                "- Contactez l'équipe AMUE si les credentials sont corrects"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_concurrent(cls, context: Dict[str, Any]) -> str:
        """
        Template pour un import concurrent détecté (verrou blue/green actif).
        """
        title = context.get('title', 'Import Concurrent Détecté')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')

        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #e65100; margin-top: 0; font-size: 18px;">
                Import Concurrent Détecté
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Un import est déjà en cours. Le DAG a été arrêté pour éviter
                une corruption des données blue/green.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>ConcurrentImportError</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-warning">annulé</span>
                </div>
            </div>
        </div>
        {traceback_html}

        <div style="background: #e3f2fd; border: 1px solid #bbdefb; padding: 15px;
                    border-radius: 4px; margin-top: 20px; font-size: 14px; color: #555;">
            <strong>Comportement attendu :</strong> Ce message est normal si un import
            manuel a été déclenché pendant un import automatique. Aucune action requise
            si l'import en cours se termine normalement.
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_WARNING)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez dans Airflow que l'import en cours se termine correctement<br>"
                "- Si le verrou est bloqué, déclenchez <code>amue_rollback</code><br>"
                "- Relancez ce DAG une fois l'import précédent terminé"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_default(cls, context: Dict[str, Any]) -> str:
        """
        Template générique (fallback). Affiche un extrait de l'erreur.
        """
        title = context.get('title', 'Erreur Détectée')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'UnknownError')
        error_message = context.get('error_message', 'Erreur inconnue')
        status = context.get('status', 'failed')

        failed_tasks = context.get('failed_tasks', [])
        failed_tasks_html = cls._render_failed_tasks(failed_tasks) if failed_tasks else ''

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur Détectée
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type d'erreur :</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">{status}</span>
                </div>
            </div>

            <div style="margin-top: 20px;">
                <strong>Message d'erreur :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}
        {failed_tasks_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(show_actions=True)
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_structure(cls, context: Dict[str, Any]) -> str:
        """
        Template pour un changement de structure de table détecté.

        Se produit quand le fingerprint d'une table a changé (colonnes ajoutées,
        supprimées, ou types modifiés côté API ou config locale).
        La table est bloquée jusqu'à validation manuelle.
        """
        title = context.get('title', 'Structure de Table Modifiée')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'StructureChangeDetected')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #fff3e0; border-left: 4px solid #FF9800;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #e65100; margin-top: 0; font-size: 18px;">
                Structure de Table Modifiée — Import Bloqué
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Le fingerprint d'une table a changé depuis le dernier import.
                La table est <strong>bloquée</strong> jusqu'à validation manuelle
                pour éviter toute corruption de données.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-warning">bloqué</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Détail :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}

        <div style="background: #e8f5e9; border: 1px solid #c8e6c9; padding: 15px;
                    border-radius: 4px; margin-top: 20px; font-size: 14px; color: #555;">
            <strong>Rappel :</strong> Un changement de fingerprint n'est pas
            forcément critique. Il peut s'agir d'une évolution intentionnelle côté API
            ou d'une mise à jour de la configuration locale. Vérifiez avant de relancer.
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_WARNING)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez les colonnes ajoutées/supprimées côté API AMUE<br>"
                "- Mettez à jour la configuration locale si le changement est attendu<br>"
                "- Relancez <code>amue_table_setup</code> pour recalculer les fingerprints<br>"
                "- Si la structure est correcte, le prochain import se débloquera automatiquement"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_table_not_found(cls, context: Dict[str, Any]) -> str:
        """
        Template pour une table configurée mais absente du statut API.

        Indique une incohérence entre la configuration Airflow (amue_tables_to_import)
        et les tables effectivement exposées par l'API AMUE.
        """
        title = context.get('title', "Table Introuvable dans l'API")
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Table(s) Introuvable(s) dans le Statut API
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Une ou plusieurs tables sont configurées dans Airflow mais absentes
                de la réponse du statut API AMUE. L'import a été arrêté.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>TableNotFoundError</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Tables manquantes :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez que les tables sont bien exposées par l'API AMUE<br>"
                "- Vérifiez la variable Airflow <code>amue_tables_to_import</code>"
                " (ou la table <code>splus_admin.amue_tables</code>)<br>"
                "- Vérifiez que le statut de polling est complet (endpoint <code>/status</code>)<br>"
                "- Retirez les tables manquantes de la configuration si elles n'existent plus"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_sync(cls, context: Dict[str, Any]) -> str:
        """
        Template pour un échec de synchronisation blue/green.

        Se produit quand la copie de tables entre schémas blue et green échoue
        sur une ou plusieurs tables.
        """
        title = context.get('title', 'Erreur de Synchronisation Blue/Green')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Synchronisation Blue/Green Échouée
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                La copie des données entre les schémas blue et green a échoué
                sur une ou plusieurs tables. Le schéma inactif peut être incomplet.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>SyncError</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Détail des erreurs :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}

        <div style="background: #fff8e1; border: 1px solid #ffe082; padding: 15px;
                    border-radius: 4px; margin-top: 20px; font-size: 14px; color: #555;">
            <strong>Impact :</strong> Le prochain import utilisera le schéma inactif
            tel quel. Si la synchronisation échoue régulièrement, relancez
            <code>amue_sync_schemas</code> manuellement avant le prochain import.
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Consultez les logs de <code>run_sync</code> pour identifier les tables en erreur<br>"
                "- Vérifiez la connexion PostgreSQL et les droits sur les schémas blue/green<br>"
                "- Relancez <code>amue_sync_schemas</code> manuellement une fois le problème résolu"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_view_switch(cls, context: Dict[str, Any]) -> str:
        """
        Template pour un échec du switch atomique des vues blue/green.

        C'est une erreur critique : les vues splus peuvent pointer vers un schéma
        incomplet ou être dans un état inconsistant. Le rollback est recommandé.
        """
        title = context.get('title', 'Erreur Critique — Switch des Vues')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #b71c1c;
                    padding: 20px; border-radius: 4px; border: 2px solid #f44336;">
            <h2 style="color: #b71c1c; margin-top: 0; font-size: 18px;">
                Erreur Critique — Switch des Vues Échoué
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Le switch atomique des vues <code>splus</code> a échoué.
                Les vues peuvent pointer vers un schéma incomplet ou être dans
                un état inconsistant. <strong>Action immédiate recommandée.</strong>
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>ViewSwitchError</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">critique</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Détail de l'erreur :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
            <div style="margin-top: 16px;">
                {traceback_html}
            </div>
        </div>
        

        <div style="background: #ffebee; border: 2px solid #f44336; padding: 15px;
                    border-radius: 4px; margin-top: 20px; font-size: 14px; color: #c62828;">
            <strong>Action immédiate :</strong> Vérifiez l'état des vues
            <code>splus</code> en base.
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez immédiatement les vues <code>splus.*</code> en base PostgreSQL<br>"
                "- Si les vues sont incohérentes, déclenchez <code>amue_rollback</code><br>"
                "- Consultez les logs de <code>switch_views</code> pour le détail de l'erreur<br>"
                "- N'importez pas de nouvelles données avant résolution"
            ),
        )
        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_error_database(cls, context: Dict[str, Any]) -> str:
        """
        Template pour les erreurs de base de données (connexion, batch insert, import).

        AMUEDatabaseError, AMUEBatchError, AMUEImportError indiquent une panne
        PostgreSQL ou un échec d'écriture — pas un bug applicatif.
        """
        title = context.get('title', 'Erreur Base de Données')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'DatabaseError')
        error_message = context.get('error_message', '')

        excerpt = cls._excerpt(error_message)
        traceback_html = cls._render_stacktrace(context.get('error_traceback'))

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336;
                    padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur Base de Données PostgreSQL
            </h2>
            <p style="color: #555; margin: 8px 0 0;">
                Une erreur s'est produite lors de l'accès ou de l'écriture en base de données.
                Les données peuvent être partiellement insérées.
            </p>

            <div class="info-grid" style="margin-top: 16px;">
                <div class="info-label">DAG :</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tâche :</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type :</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut :</div>
                <div class="info-value">
                    <span class="badge badge-error">failed</span>
                </div>
            </div>

            <div style="margin-top: 16px;">
                <strong>Extrait de l'erreur :</strong>
                <div class="message-box">{cls.escape_html(excerpt)}</div>
            </div>
        </div>
        {traceback_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(
            show_actions=True,
            actions=(
                "- Vérifiez la connexion PostgreSQL (host, port, credentials, pool)<br>"
                "- Consultez les logs Airflow pour l'erreur SQL précise<br>"
                "- Vérifiez que le schéma cible et les tables existent<br>"
                "- Relancez le DAG une fois la base de données disponible"
            ),
        )
        return cls._wrap_html(header, content, footer)

    # ------------------------------------------------------------------
    # Composants partagés
    # ------------------------------------------------------------------

    @classmethod
    def _render_failed_tasks(cls, tasks: List[Dict[str, Any]]) -> str:
        """Rendu du tableau des tâches en échec."""
        rows_html = ''
        for t in tasks:
            task_id = t.get('task_id', 'unknown')
            map_index = t.get('map_index', -1)
            label = f"{task_id}[{map_index}]" if map_index >= 0 else task_id
            duration = t.get('duration')
            dur_str = f"{duration}s" if duration is not None else 'N/A'
            rows_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;
                           font-family: 'Courier New', monospace;">
                    {label}
                </td>
                <td style="padding: 8px; border-bottom: 1px solid #e0e0e0;
                           text-align: center;">
                    {dur_str}
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 20px;">
            <strong>Tâches en échec :</strong>
            <table style="width: 100%; border-collapse: collapse; margin-top: 8px;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="padding: 8px; text-align: left;
                                   border-bottom: 2px solid #e0e0e0;">Tâche</th>
                        <th style="padding: 8px; text-align: center;
                                   border-bottom: 2px solid #e0e0e0;">Durée</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
        """

    @staticmethod
    def _excerpt(message: str) -> str:
        """Retourne le message complet, ou 'Erreur inconnue' si vide."""
        return message if message else 'Erreur inconnue'
