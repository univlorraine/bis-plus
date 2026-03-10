"""
Gestionnaire d'état centralisé en BDD pour le projet AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Remplace les variables Airflow d'état par une table PostgreSQL dédiée :
    splus_admin.amue_state (une seule ligne, id=1)

Variables remplacées :
    - amue_last_finish_timestamp  → last_finish_timestamp
    - amue_last_successful_run    → last_successful_run
    - amue_bluegreen_state (JSON) → colonnes typées (active_schema, ...)

AVANTAGE principal : l'acquisition du verrou d'import est atomique via
un UPDATE ... WHERE import_in_progress = FALSE RETURNING id.

================================================================================
"""
import logging
from datetime import datetime
from typing import Optional

from amue.utils.database.hooks import create_postgres_hook
from amue.utils.tracing import to_iso_str

logger = logging.getLogger(__name__)

_TABLE = "splus_admin.amue_state"
_ROW_ID = 1


class AdminStateManager:
    """
    Accès à l'état centralisé stocké dans splus_admin.amue_state.

    Toutes les méthodes utilisent des requêtes SQL directes sur la table
    (pas de cache) pour garantir la fraîcheur des données.

    Example:
        >>> mgr = AdminStateManager()
        >>> ts = mgr.get_last_finish_timestamp()
        >>> mgr.set_last_finish_timestamp('2026-02-17T10:18:22+00:00')
    """

    def __init__(self, postgres_hook=None):
        self._hook = postgres_hook or create_postgres_hook(schema='public')

    # =========================================================================
    # TIMESTAMPS DE SYNCHRO
    # =========================================================================

    def get_last_finish_timestamp(self) -> Optional[str]:
        """
        Retourne le dernier timestamp finish AMUE (ISO 8601) ou None.

        Returns:
            Chaîne ISO 8601 ou None si jamais enregistré
        """
        try:
            row = self._hook.get_first(
                f"SELECT last_finish_timestamp FROM {_TABLE} WHERE id = %s",
                parameters=(_ROW_ID,)
            )
            if row and row[0] is not None:
                return to_iso_str(row[0])
            return None
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible de lire last_finish_timestamp: {e}")
            return None

    def set_last_finish_timestamp(self, ts: str) -> None:
        """
        Enregistre le timestamp finish AMUE.

        Args:
            ts: Valeur ISO 8601 retournée par l'API AMUE
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET last_finish_timestamp = %s, updated_at = NOW() WHERE id = %s",
                parameters=(ts, _ROW_ID)
            )
            logger.info(f"[ADMIN_STATE] last_finish_timestamp mis à jour: {ts}")
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible d'écrire last_finish_timestamp: {e}")

    def get_last_successful_run(self) -> Optional[str]:
        """
        Retourne la date du dernier import global réussi (ISO 8601) ou None.

        Returns:
            Chaîne ISO 8601 ou None
        """
        try:
            row = self._hook.get_first(
                f"SELECT last_successful_run FROM {_TABLE} WHERE id = %s",
                parameters=(_ROW_ID,)
            )
            if row and row[0] is not None:
                return to_iso_str(row[0])
            return None
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible de lire last_successful_run: {e}")
            return None

    def set_last_successful_run(self, ts: str) -> None:
        """
        Enregistre la date du dernier import global réussi.

        Args:
            ts: Valeur ISO 8601
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET last_successful_run = %s, updated_at = NOW() WHERE id = %s",
                parameters=(ts, _ROW_ID)
            )
            logger.info(f"[ADMIN_STATE] last_successful_run mis à jour: {ts}")
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible d'écrire last_successful_run: {e}")

    def get_last_report_start(self) -> Optional[str]:
        """
        Retourne le timestamp de début du dernier rapport AMUE traité (ISO 8601) ou None.

        Ce timestamp est le champ 'start' retourné par l'API AMUE et sert de
        référence pour les imports différentiels : toutes les tables delta
        filtrent leurs données avec delta_column >= last_report_start.

        Returns:
            Chaîne ISO 8601 ou None si jamais enregistré
        """
        try:
            row = self._hook.get_first(
                f"SELECT last_report_start FROM {_TABLE} WHERE id = %s",
                parameters=(_ROW_ID,)
            )
            if row and row[0] is not None:
                return to_iso_str(row[0])
            return None
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible de lire last_report_start: {e}")
            return None

    def set_last_report_start(self, ts: str) -> None:
        """
        Enregistre le timestamp de début du rapport AMUE.

        Args:
            ts: Valeur ISO 8601 du champ 'start' retourné par l'API AMUE
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET last_report_start = %s, updated_at = NOW() WHERE id = %s",
                parameters=(ts, _ROW_ID)
            )
            logger.info(f"[ADMIN_STATE] last_report_start mis à jour: {ts}")
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible d'écrire last_report_start: {e}")

    # =========================================================================
    # ÉTAT BLUE/GREEN
    # =========================================================================

    def get_bluegreen_state(self):
        """
        Retourne l'état blue/green depuis la BDD sous forme de BlueGreenState.

        Import lazy de BlueGreenState pour éviter les imports circulaires.

        Returns:
            BlueGreenState ou None si la table est inaccessible
        """
        from amue.services.bluegreen.bluegreen_manager import BlueGreenState
        try:
            row = self._hook.get_first(
                f"""
                SELECT active_schema, last_switch_timestamp, last_sync_timestamp,
                       import_in_progress, import_started_at, import_correlation_id
                FROM {_TABLE} WHERE id = %s
                """,
                parameters=(_ROW_ID,)
            )
            if not row:
                return BlueGreenState()
            return BlueGreenState(
                last_import_schema=row[0] or "",
                last_switch_timestamp=to_iso_str(row[1]) or "",
                last_sync_timestamp=to_iso_str(row[2]) or "",
                import_in_progress=bool(row[3]),
                import_started_at=to_iso_str(row[4]) or "",
                import_correlation_id=row[5] or "",
            )
        except Exception as e:
            logger.warning(f"[ADMIN_STATE] Impossible de lire l'état blue/green: {e}")
            from amue.services.bluegreen.bluegreen_manager import BlueGreenState
            return BlueGreenState()

    def save_bluegreen_state(self, state) -> bool:
        """
        Sauvegarde l'état blue/green complet en BDD.

        Args:
            state: BlueGreenState à persister

        Returns:
            True si succès
        """
        try:
            self._hook.run(
                f"""
                UPDATE {_TABLE} SET
                    active_schema         = %s,
                    last_switch_timestamp = %s,
                    last_sync_timestamp   = %s,
                    import_in_progress    = %s,
                    import_started_at     = %s,
                    import_correlation_id = %s,
                    updated_at            = NOW()
                WHERE id = %s
                """,
                parameters=(
                    state.last_import_schema or None,
                    state.last_switch_timestamp or None,
                    state.last_sync_timestamp or None,
                    state.import_in_progress,
                    state.import_started_at or None,
                    state.import_correlation_id or None,
                    _ROW_ID,
                )
            )
            logger.info(f"[ADMIN_STATE] État blue/green sauvegardé")
            return True
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur sauvegarde état blue/green: {e}")
            return False

    # =========================================================================
    # GESTION DU VERROU D'IMPORT (atomique via PostgreSQL)
    # =========================================================================

    def try_acquire_import_lock(self, started_at: str, correlation_id: str) -> bool:
        """
        Tente d'acquérir le verrou d'import de manière atomique.

        Utilise UPDATE ... WHERE import_in_progress = FALSE RETURNING id
        pour garantir l'exclusivité sans race condition.

        Args:
            started_at: Timestamp ISO du début de l'import
            correlation_id: ID de corrélation pour tracer l'import

        Returns:
            True si le verrou a été acquis, False si déjà verrouillé
        """
        try:
            rows = self._hook.get_records(
                f"""
                UPDATE {_TABLE}
                SET import_in_progress    = TRUE,
                    import_started_at     = %s,
                    import_correlation_id = %s,
                    updated_at            = NOW()
                WHERE id = %s AND import_in_progress = FALSE
                RETURNING id
                """,
                parameters=(started_at, correlation_id, _ROW_ID)
            )
            acquired = bool(rows)
            if acquired:
                logger.info(f"[ADMIN_STATE] Verrou acquis (correlation_id: {correlation_id or 'N/A'})")
            return acquired
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur acquisition verrou: {e}")
            return False

    def release_import_lock(self, active_schema: str) -> bool:
        """
        Libère le verrou d'import et enregistre le schéma importé.

        N'effectue l'UPDATE que si le verrou est effectivement tenu
        (import_in_progress = TRUE). Retourne False si le verrou n'était pas tenu.

        Args:
            active_schema: Nom court du schéma qui vient d'être importé (ex: 'blue')

        Returns:
            True si le verrou a été libéré, False si non tenu ou erreur
        """
        try:
            result = self._hook.get_first(
                f"""
                UPDATE {_TABLE}
                SET import_in_progress    = FALSE,
                    active_schema         = %s,
                    import_started_at     = NULL,
                    import_correlation_id = NULL,
                    updated_at            = NOW()
                WHERE id = %s AND import_in_progress = TRUE
                RETURNING id
                """,
                parameters=(active_schema or None, _ROW_ID)
            )
            if result is None:
                logger.warning("[ADMIN_STATE] release_import_lock: verrou non tenu, aucune ligne mise à jour")
                return False
            logger.info(f"[ADMIN_STATE] Verrou libéré (schéma: {active_schema})")
            return True
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur libération verrou: {e}")
            return False

    def force_release_lock(self) -> bool:
        """
        Force la libération du verrou sans modifier active_schema.

        Utilisé pour les verrous abandonnés (stale locks).

        Returns:
            True si succès
        """
        try:
            self._hook.run(
                f"""
                UPDATE {_TABLE}
                SET import_in_progress    = FALSE,
                    import_started_at     = NULL,
                    import_correlation_id = NULL,
                    updated_at            = NOW()
                WHERE id = %s
                """,
                parameters=(_ROW_ID,)
            )
            logger.warning("[ADMIN_STATE] Verrou forcément libéré")
            return True
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur libération forcée: {e}")
            return False

    def mark_switch_completed(self, active_schema: str) -> bool:
        """
        Enregistre la fin d'un switch de vues.

        Args:
            active_schema: Nom court du nouveau schéma actif (ex: 'green')

        Returns:
            True si succès
        """
        try:
            self._hook.run(
                f"""
                UPDATE {_TABLE}
                SET last_switch_timestamp = NOW(),
                    active_schema         = %s,
                    updated_at            = NOW()
                WHERE id = %s
                """,
                parameters=(active_schema or None, _ROW_ID)
            )
            logger.info(f"[ADMIN_STATE] Switch enregistré (nouveau actif: {active_schema})")
            return True
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur mark_switch_completed: {e}")
            return False

    def update_import_timestamps(
        self,
        finish_timestamp: Optional[str] = None,
        report_start: Optional[str] = None,
        last_successful_run: Optional[str] = None,
    ) -> None:
        """
        Met à jour les trois timestamps post-import en une seule transaction atomique.

        Args:
            finish_timestamp: Timestamp finish de l'API AMUE (ISO 8601)
            report_start: Timestamp start du rapport AMUE (ISO 8601)
            last_successful_run: Date du dernier succès global (ISO 8601)

        Raises:
            Exception: Si l'UPDATE échoue
        """
        self._hook.run(
            f"""
            UPDATE {_TABLE} SET
                last_finish_timestamp = COALESCE(%s, last_finish_timestamp),
                last_report_start     = COALESCE(%s, last_report_start),
                last_successful_run   = COALESCE(%s, last_successful_run),
                updated_at            = NOW()
            WHERE id = %s
            """,
            parameters=(finish_timestamp, report_start, last_successful_run, _ROW_ID)
        )
        logger.info(
            f"[ADMIN_STATE] Timestamps mis à jour (finish={finish_timestamp},"
            f" report_start={report_start}, last_success={last_successful_run})"
        )

    def mark_sync_completed(self) -> bool:
        """
        Enregistre la fin d'une synchronisation blue/green.

        Returns:
            True si succès
        """
        try:
            self._hook.run(
                f"UPDATE {_TABLE} SET last_sync_timestamp = NOW(), updated_at = NOW() WHERE id = %s",
                parameters=(_ROW_ID,)
            )
            logger.info("[ADMIN_STATE] Sync enregistrée")
            return True
        except Exception as e:
            logger.error(f"[ADMIN_STATE] Erreur mark_sync_completed: {e}")
            return False
