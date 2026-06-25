"""
Layer: infrastructure

Helpers de tracing partagés AMUE / ECC.

Fonctionnalités :
    1. CORRELATION IDs : génération d'IDs courts pour tracer les opérations
    2. MEMORY TRACKING : suivi consommation mémoire via tracemalloc
    3. TIMING : mesure de la durée des opérations
    4. CONTEXTE COMPLET : combine les trois dans un context manager unique

Usage :
    from common.infrastructure.observability.tracing import (
        generate_correlation_id,
        MemoryTracker,
        OperationTimer,
        TracingContext,
        to_iso_str,
    )
"""
import logging
import time
import tracemalloc
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# ISO STRING HELPER
# =============================================================================

def to_iso_str(v) -> Optional[str]:
    """Convertit une valeur datetime-like en chaîne ISO 8601, ou None."""
    if v is None:
        return None
    return v.isoformat() if hasattr(v, 'isoformat') else str(v)


# =============================================================================
# CORRELATION IDs
# =============================================================================

def generate_correlation_id(prefix: str = "") -> str:
    """
    Génère un ID de corrélation unique court (8 caractères).

    Args:
        prefix: Préfixe optionnel (ex: "import", "sync")

    Returns:
        ID de corrélation (ex: "import-a1b2c3d4" ou "a1b2c3d4")
    """
    short_id = str(uuid.uuid4())[:8]
    if prefix:
        return f"{prefix}-{short_id}"
    return short_id


def generate_run_id() -> str:
    """Génère un ID de run basé sur le timestamp (YYYYMMDD-HHMMSS-XXXX)."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    short_uuid = str(uuid.uuid4())[:4]
    return f"{timestamp}-{short_uuid}"


# =============================================================================
# MEMORY TRACKING
# =============================================================================

@dataclass
class MemorySnapshot:
    """Capture de l'état mémoire à un instant donné."""
    timestamp: datetime
    current_mb: float
    peak_mb: float
    label: str = ""


class MemoryTracker:
    """
    Tracker de consommation mémoire (tracemalloc).

    Example:
        >>> with MemoryTracker("import_csks") as tracker:
        ...     # ... opérations ...
        >>> print(f"Peak: {tracker.peak_mb:.1f} MB")
    """

    def __init__(self, operation_name: str = "operation"):
        self.operation_name = operation_name
        self.started = False
        self.snapshots: List[MemorySnapshot] = []
        self._start_current: float = 0
        self._start_peak: float = 0

    def start(self) -> None:
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
        if not self.started:
            return
        current, peak = tracemalloc.get_traced_memory()
        current_mb = current / 1024 / 1024
        peak_mb = peak / 1024 / 1024
        self.snapshots.append(MemorySnapshot(
            timestamp=datetime.now(),
            current_mb=current_mb,
            peak_mb=peak_mb,
            label="final",
        ))
        self.started = False
        delta_current = current_mb - self._start_current
        logger.info(
            f"[MEMORY] Stop {self.operation_name}: "
            f"current={current_mb:.1f}MB (+{delta_current:.1f}MB), "
            f"peak={peak_mb:.1f}MB"
        )

    def snapshot(self, label: str = "") -> MemorySnapshot:
        if not self.started:
            self.start()
        current, peak = tracemalloc.get_traced_memory()
        snap = MemorySnapshot(
            timestamp=datetime.now(),
            current_mb=current / 1024 / 1024,
            peak_mb=peak / 1024 / 1024,
            label=label,
        )
        self.snapshots.append(snap)
        return snap

    @property
    def current_mb(self) -> float:
        if not tracemalloc.is_tracing():
            return 0
        current, _ = tracemalloc.get_traced_memory()
        return current / 1024 / 1024

    @property
    def peak_mb(self) -> float:
        if not tracemalloc.is_tracing():
            return 0
        _, peak = tracemalloc.get_traced_memory()
        return peak / 1024 / 1024

    def __enter__(self) -> 'MemoryTracker':
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


# =============================================================================
# TIMING
# =============================================================================

@dataclass
class TimingResult:
    """Résultat d'une mesure de temps."""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    @property
    def duration_human(self) -> str:
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
        self.operation = operation
        self.result: Optional[TimingResult] = None
        self._start: float = 0

    def start(self) -> None:
        self._start = time.perf_counter()
        self.result = TimingResult(operation=self.operation, start_time=datetime.now())

    def stop(self) -> TimingResult:
        if self.result is None:
            self.start()
        self.result.end_time = datetime.now()
        self.result.duration_seconds = time.perf_counter() - self._start
        return self.result

    @property
    def duration_seconds(self) -> float:
        if self.result:
            return self.result.duration_seconds
        return time.perf_counter() - self._start

    @property
    def duration_human(self) -> str:
        if self.result:
            return self.result.duration_human
        return TimingResult(
            operation=self.operation,
            start_time=datetime.now(),
            duration_seconds=self.duration_seconds,
        ).duration_human

    def __enter__(self) -> 'OperationTimer':
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        logger.debug(f"[TIMING] {self.operation}: {self.duration_human}")


# =============================================================================
# CONTEXTE DE TRACING COMPLET
# =============================================================================

@dataclass
class TracingContext:
    """
    Contexte de tracing complet (correlation ID + memory + timing).

    Example:
        >>> with TracingContext("import_csks") as ctx:
        ...     logger.info(f"[{ctx.correlation_id}] Début")
        >>> print(ctx.summary())
    """

    operation: str
    correlation_id: str = field(default_factory=lambda: generate_correlation_id())
    parent_correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    _memory_tracker: Optional[MemoryTracker] = field(default=None, repr=False)
    _timer: Optional[OperationTimer] = field(default=None, repr=False)
    _started: bool = field(default=False, repr=False)

    def __post_init__(self):
        self._memory_tracker = MemoryTracker(self.operation)
        self._timer = OperationTimer(self.operation)

    def start(self) -> None:
        self._started = True
        self._timer.start()
        self._memory_tracker.start()
        logger.info(f"[{self.correlation_id}] Début: {self.operation}")

    def stop(self) -> None:
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
        self.metadata[key] = value

    @property
    def duration_seconds(self) -> float:
        return self._timer.duration_seconds if self._timer else 0

    @property
    def peak_memory_mb(self) -> float:
        return self._memory_tracker.peak_mb if self._memory_tracker else 0

    def summary(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "correlation_id": self.correlation_id,
            "parent_correlation_id": self.parent_correlation_id,
            "duration_seconds": self.duration_seconds,
            "duration_human": self._timer.duration_human if self._timer else "N/A",
            "peak_memory_mb": self.peak_memory_mb,
            "metadata": self.metadata,
        }

    def __enter__(self) -> 'TracingContext':
        # Bind le correlation_id au contexte structlog si activé (no-op sinon)
        from common.infrastructure.observability.structured_logging import is_enabled
        if is_enabled():
            import structlog
            self._structlog_tokens = structlog.contextvars.bind_contextvars(
                correlation_id=self.correlation_id,
                operation=self.operation,
            )
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.add_metadata("error", str(exc_val))
            self.add_metadata("error_type", exc_type.__name__)
        self.stop()
        # Détache le correlation_id du contexte structlog
        tokens = getattr(self, '_structlog_tokens', None)
        if tokens:
            import structlog
            structlog.contextvars.reset_contextvars(**tokens)
            self._structlog_tokens = None


# =============================================================================
# HELPERS
# =============================================================================

def log_with_correlation(
    logger_instance,
    level: str,
    message: str,
    correlation_id: Optional[str] = None,
    **kwargs,
) -> None:
    """Log un message avec correlation ID préfixé."""
    if correlation_id:
        message = f"[{correlation_id}] {message}"
    log_method = getattr(logger_instance, level.lower(), logger_instance.info)
    log_method(message, **kwargs)


@contextmanager
def trace_operation(operation: str, correlation_id: Optional[str] = None):
    """
    Context manager simple pour tracer une opération.

    Example:
        >>> with trace_operation("fetch_data") as (cid, timer):
        ...     logger.info(f"[{cid}] Fetching...")
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
