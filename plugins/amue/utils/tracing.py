"""
Module de tracing et monitoring pour AMUE.

================================================================================
FONCTIONNALITÉS
================================================================================

1. CORRELATION IDs
   - Génère des IDs uniques pour tracer les opérations
   - Permet de suivre une opération à travers plusieurs composants
   - Facilite le debugging et l'analyse des logs

2. MEMORY MONITORING
   - Suit la consommation mémoire pendant les imports
   - Détecte les pics de mémoire
   - Permet d'ajuster les tailles de batch dynamiquement

3. TIMING
   - Mesure la durée des opérations
   - Fournit des statistiques de performance

================================================================================
USAGE
================================================================================

    from amue.utils.tracing import (
        generate_correlation_id,
        MemoryTracker,
        OperationTimer,
        TracingContext
    )

    # Correlation ID
    correlation_id = generate_correlation_id()
    logger.info(f"[{correlation_id}] Début de l'opération")

    # Memory tracking
    with MemoryTracker("import_table") as tracker:
        import_data()
    print(f"Mémoire peak: {tracker.peak_mb:.1f} MB")

    # Timing
    with OperationTimer("fetch_data") as timer:
        fetch_data()
    print(f"Durée: {timer.duration_seconds:.2f}s")

    # Context complet
    with TracingContext("import_csks") as ctx:
        # ctx.correlation_id disponible
        import_data()
    print(ctx.summary())

================================================================================
"""
import logging
import time
import tracemalloc
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List, Any

logger = logging.getLogger(__name__)


# =============================================================================
# CORRELATION IDs
# =============================================================================

def generate_correlation_id(prefix: str = "") -> str:
    """
    Génère un ID de corrélation unique.

    L'ID est court (8 caractères) pour être facilement lisible dans les logs
    tout en étant suffisamment unique pour une session.

    Args:
        prefix: Préfixe optionnel (ex: "import", "sync")

    Returns:
        ID de corrélation (ex: "import-a1b2c3d4" ou "a1b2c3d4")

    Example:
        >>> cid = generate_correlation_id("import")
        >>> logger.info(f"[{cid}] Début import")
        [import-a1b2c3d4] Début import
    """
    short_id = str(uuid.uuid4())[:8]
    if prefix:
        return f"{prefix}-{short_id}"
    return short_id


def generate_run_id() -> str:
    """
    Génère un ID de run basé sur le timestamp.

    Format: YYYYMMDD-HHMMSS-XXXX (ex: 20240115-143052-a1b2)

    Returns:
        ID de run unique
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    short_uuid = str(uuid.uuid4())[:4]
    return f"{timestamp}-{short_uuid}"


# =============================================================================
# MEMORY TRACKING
# =============================================================================

@dataclass
class MemorySnapshot:
    """Capture de l'état mémoire à un instant donné"""
    timestamp: datetime
    current_mb: float
    peak_mb: float
    label: str = ""


class MemoryTracker:
    """
    Tracker de consommation mémoire.

    Utilise tracemalloc pour suivre l'allocation mémoire Python.

    Example:
        >>> tracker = MemoryTracker("import_csks")
        >>> tracker.start()
        >>> # ... opérations ...
        >>> tracker.stop()
        >>> print(f"Peak: {tracker.peak_mb:.1f} MB")
    """

    def __init__(self, operation_name: str = "operation"):
        """
        Initialise le tracker.

        Args:
            operation_name: Nom de l'opération (pour les logs)
        """
        self.operation_name = operation_name
        self.started = False
        self.snapshots: List[MemorySnapshot] = []
        self._start_current: float = 0
        self._start_peak: float = 0

    def start(self) -> None:
        """Démarre le tracking mémoire"""
        if not tracemalloc.is_tracing():
            tracemalloc.start()

        current, peak = tracemalloc.get_traced_memory()
        self._start_current = current / 1024 / 1024
        self._start_peak = peak / 1024 / 1024
        self.started = True

        logger.debug(
            f"[MEMORY] Start {self.operation_name}: "
            f"current={self._start_current:.1f}MB, peak={self._start_peak:.1f}MB"
        )

    def stop(self) -> None:
        """Arrête le tracking et enregistre le snapshot final"""
        if not self.started:
            return

        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / 1024 / 1024
        peak_mb = peak / 1024 / 1024

        self.snapshots.append(MemorySnapshot(
            timestamp=datetime.now(),
            current_mb=current_mb,
            peak_mb=peak_mb,
            label="final"
        ))

        self.started = False

        # Log le résultat
        delta_current = current_mb - self._start_current
        logger.info(
            f"[MEMORY] Stop {self.operation_name}: "
            f"current={current_mb:.1f}MB (+{delta_current:.1f}MB), "
            f"peak={peak_mb:.1f}MB"
        )

    def snapshot(self, label: str = "") -> MemorySnapshot:
        """
        Prend un snapshot intermédiaire.

        Args:
            label: Label pour identifier le snapshot

        Returns:
            Le snapshot pris
        """
        if not self.started:
            self.start()

        current, peak = tracemalloc.get_traced_memory()
        snap = MemorySnapshot(
            timestamp=datetime.now(),
            current_mb=current / 1024 / 1024,
            peak_mb=peak / 1024 / 1024,
            label=label
        )
        self.snapshots.append(snap)
        return snap

    @property
    def current_mb(self) -> float:
        """Mémoire actuelle en MB"""
        if not tracemalloc.is_tracing():
            return 0
        current, _ = tracemalloc.get_traced_memory()
        return current / 1024 / 1024

    @property
    def peak_mb(self) -> float:
        """Pic mémoire en MB depuis le début du tracking"""
        if not tracemalloc.is_tracing():
            return 0
        _, peak = tracemalloc.get_traced_memory()
        return peak / 1024 / 1024

    def __enter__(self) -> 'MemoryTracker':
        """Context manager: démarre le tracking"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager: arrête le tracking"""
        self.stop()


# =============================================================================
# TIMING
# =============================================================================

@dataclass
class TimingResult:
    """Résultat d'une mesure de temps"""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    @property
    def duration_human(self) -> str:
        """Durée formatée pour affichage humain"""
        if self.duration_seconds < 1:
            return f"{self.duration_seconds * 1000:.0f}ms"
        elif self.duration_seconds < 60:
            return f"{self.duration_seconds:.1f}s"
        elif self.duration_seconds < 3600:
            minutes = int(self.duration_seconds // 60)
            seconds = int(self.duration_seconds % 60)
            return f"{minutes}m{seconds}s"
        else:
            hours = int(self.duration_seconds // 3600)
            minutes = int((self.duration_seconds % 3600) // 60)
            return f"{hours}h{minutes}m"


class OperationTimer:
    """
    Timer pour mesurer la durée des opérations.

    Example:
        >>> with OperationTimer("fetch_data") as timer:
        ...     fetch_data()
        >>> print(f"Durée: {timer.duration_human}")
    """

    def __init__(self, operation: str):
        """
        Initialise le timer.

        Args:
            operation: Nom de l'opération
        """
        self.operation = operation
        self.result: Optional[TimingResult] = None
        self._start: float = 0

    def start(self) -> None:
        """Démarre le timer"""
        self._start = time.perf_counter()
        self.result = TimingResult(
            operation=self.operation,
            start_time=datetime.now()
        )

    def stop(self) -> TimingResult:
        """
        Arrête le timer.

        Returns:
            Résultat avec la durée
        """
        if self.result is None:
            self.start()

        self.result.end_time = datetime.now()
        self.result.duration_seconds = time.perf_counter() - self._start
        return self.result

    @property
    def duration_seconds(self) -> float:
        """Durée en secondes"""
        if self.result:
            return self.result.duration_seconds
        return time.perf_counter() - self._start

    @property
    def duration_human(self) -> str:
        """Durée formatée pour affichage humain"""
        if self.result:
            return self.result.duration_human
        # Timer en cours
        return TimingResult(
            operation=self.operation,
            start_time=datetime.now(),
            duration_seconds=self.duration_seconds
        ).duration_human

    def __enter__(self) -> 'OperationTimer':
        """Context manager: démarre le timer"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager: arrête le timer"""
        self.stop()
        logger.debug(f"[TIMING] {self.operation}: {self.duration_human}")


# =============================================================================
# CONTEXT DE TRACING COMPLET
# =============================================================================

@dataclass
class TracingContext:
    """
    Contexte de tracing complet pour une opération.

    Combine correlation ID, memory tracking et timing.

    Example:
        >>> with TracingContext("import_csks") as ctx:
        ...     logger.info(f"[{ctx.correlation_id}] Début import")
        ...     import_data()
        >>> print(ctx.summary())
    """

    operation: str
    correlation_id: str = field(default_factory=lambda: generate_correlation_id())
    parent_correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Composants internes
    _memory_tracker: Optional[MemoryTracker] = field(default=None, repr=False)
    _timer: Optional[OperationTimer] = field(default=None, repr=False)
    _started: bool = field(default=False, repr=False)

    def __post_init__(self):
        """Initialise les composants"""
        self._memory_tracker = MemoryTracker(self.operation)
        self._timer = OperationTimer(self.operation)

    def start(self) -> None:
        """Démarre le tracing"""
        self._started = True
        self._timer.start()
        self._memory_tracker.start()
        logger.info(f"[{self.correlation_id}] Début: {self.operation}")

    def stop(self) -> None:
        """Arrête le tracing"""
        if not self._started:
            return

        self._timer.stop()
        self._memory_tracker.stop()
        self._started = False
        logger.info(
            f"[{self.correlation_id}] Fin: {self.operation} "
            f"({self._timer.duration_human}, peak={self._memory_tracker.peak_mb:.1f}MB)"
        )

    def add_metadata(self, key: str, value: Any) -> None:
        """Ajoute une métadonnée au contexte"""
        self.metadata[key] = value

    @property
    def duration_seconds(self) -> float:
        """Durée en secondes"""
        return self._timer.duration_seconds if self._timer else 0

    @property
    def peak_memory_mb(self) -> float:
        """Pic mémoire en MB"""
        return self._memory_tracker.peak_mb if self._memory_tracker else 0

    def summary(self) -> Dict[str, Any]:
        """
        Retourne un résumé du tracing.

        Returns:
            Dict avec toutes les informations de tracing
        """
        return {
            "operation": self.operation,
            "correlation_id": self.correlation_id,
            "parent_correlation_id": self.parent_correlation_id,
            "duration_seconds": self.duration_seconds,
            "duration_human": self._timer.duration_human if self._timer else "N/A",
            "peak_memory_mb": self.peak_memory_mb,
            "metadata": self.metadata
        }

    def __enter__(self) -> 'TracingContext':
        """Context manager: démarre le tracing"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager: arrête le tracing"""
        if exc_type:
            self.add_metadata("error", str(exc_val))
            self.add_metadata("error_type", exc_type.__name__)
        self.stop()


# =============================================================================
# HELPERS
# =============================================================================

def log_with_correlation(
    logger_instance,
    level: str,
    message: str,
    correlation_id: Optional[str] = None,
    **kwargs
) -> None:
    """
    Log un message avec correlation ID.

    Args:
        logger_instance: Logger à utiliser
        level: Niveau de log (debug, info, warning, error)
        message: Message à logger
        correlation_id: ID de corrélation (optionnel)
        **kwargs: Arguments additionnels pour le logger
    """
    if correlation_id:
        message = f"[{correlation_id}] {message}"

    log_method = getattr(logger_instance, level.lower(), logger_instance.info)
    log_method(message, **kwargs)


@contextmanager
def trace_operation(operation: str, correlation_id: Optional[str] = None):
    """
    Context manager simple pour tracer une opération.

    Args:
        operation: Nom de l'opération
        correlation_id: ID de corrélation (généré si non fourni)

    Yields:
        Tuple (correlation_id, timer)

    Example:
        >>> with trace_operation("fetch_data") as (cid, timer):
        ...     logger.info(f"[{cid}] Fetching...")
        ...     data = fetch()
        >>> # Log automatique de la durée à la fin
    """
    cid = correlation_id or generate_correlation_id()
    timer = OperationTimer(operation)

    logger.debug(f"[{cid}] Start: {operation}")
    timer.start()

    try:
        yield cid, timer
    finally:
        timer.stop()
        logger.debug(f"[{cid}] End: {operation} ({timer.duration_human})")
