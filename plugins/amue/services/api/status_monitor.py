"""
Service de monitoring du statut API AMUE.

================================================================================
RÔLE DU MODULE
================================================================================

Encapsule la boucle de surveillance du statut API AMUE :
    - Poll l'API à intervalle régulier
    - Log chaque changement de réponse (JSON complet)
    - S'arrête automatiquement après la durée maximale

Extrait de dag_amue_status_monitor.py pour être testable
indépendamment d'Airflow.

================================================================================
"""
import json
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StatusMonitor:
    """
    Surveille les changements de statut de l'API AMUE.

    Poll l'API périodiquement et log les changements de réponse.
    Silencieux si la réponse est identique à la précédente.

    Example:
        >>> monitor = StatusMonitor(checker, duration_hours=4, poll_interval_seconds=60)
        >>> monitor.watch()
    """

    def __init__(
        self,
        checker,
        duration_hours: int = 4,
        poll_interval_seconds: int = 60,
    ):
        """
        Initialise le monitor.

        Args:
            checker: Instance de AMUEStatusChecker
            duration_hours: Durée totale de surveillance (défaut: 4h)
            poll_interval_seconds: Intervalle entre les polls (défaut: 60s)
        """
        self._checker = checker
        self._duration_hours = duration_hours
        self._poll_interval_seconds = poll_interval_seconds

    def watch(self) -> None:
        """
        Lance la boucle de surveillance jusqu'à expiration de la durée.

        Log chaque changement de réponse API au niveau INFO.
        Les erreurs API sont loggées au niveau WARNING mais n'arrêtent pas la boucle.
        """
        deadline = datetime.now() + timedelta(hours=self._duration_hours)
        previous_snapshot: str | None = None

        logger.info(
            f"[MONITOR] Démarrage — surveillance jusqu'à {deadline:%H:%M:%S} "
            f"(intervalle {self._poll_interval_seconds}s)"
        )

        while datetime.now() < deadline:
            now_str = datetime.now().strftime('%H:%M:%S')

            try:
                result = self._checker.fetch_full_status()
                raw = result.get('raw_response') or result
                snapshot = json.dumps(raw, sort_keys=True, default=str)

                if snapshot != previous_snapshot:
                    logger.info(
                        f"[MONITOR] {now_str} — CHANGEMENT DÉTECTÉ\n"
                        + json.dumps(raw, indent=2, default=str)
                    )
                    previous_snapshot = snapshot

            except Exception as exc:
                logger.warning(f"[MONITOR] {now_str} — Erreur API: {exc}")

            time.sleep(self._poll_interval_seconds)

        logger.info(f"[MONITOR] Fin de surveillance après {self._duration_hours}h")
