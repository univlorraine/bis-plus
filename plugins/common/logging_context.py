"""
Contexte de logging partagé entre AMUE et ECC.

Utilise des ContextVars (thread-safe, coroutine-safe) pour propager
automatiquement le correlation_id dans tous les logs sans modifier
les 70+ appels logger.xxx existants.

Usage :
    # Au démarrage d'un DAG run (init_bluegreen)
    from common.logging_context import set_correlation_id
    set_correlation_id(dag_run.run_id)

    # Dans n'importe quel module (lecture automatique)
    from common.logging_context import CorrelationIdFilter
    handler.addFilter(CorrelationIdFilter())

Format de log cible :
    2026-03-09 10:00:00 [INFO] [import-a1b2c3d4] [module.name] [IMPORT] Table CSKS
"""
import logging
from contextvars import ContextVar

_correlation_id_ctx: ContextVar[str] = ContextVar('correlation_id', default='N/A')


class CorrelationIdFilter(logging.Filter):
    """
    Filtre logging qui injecte automatiquement le correlation_id dans chaque LogRecord.

    Ajoute l'attribut ``correlation_id`` sans modifier les appels logger existants.

    Example:
        >>> handler = logging.StreamHandler()
        >>> handler.addFilter(CorrelationIdFilter())
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _correlation_id_ctx.get()
        return True


def set_correlation_id(cid: str) -> None:
    """
    Définit le correlation_id du contexte courant (thread + coroutine safe).

    Args:
        cid: Identifiant de corrélation (ex: dag_run.run_id, 'import-a1b2c3d4')
    """
    _correlation_id_ctx.set(cid)


def get_correlation_id() -> str:
    """
    Retourne le correlation_id du contexte courant.

    Returns:
        Identifiant de corrélation ou 'N/A' si non défini
    """
    return _correlation_id_ctx.get()
