"""
Préfixes de logging standardisés pour AMUE et ECC.

Centralise les 38 préfixes [MODULE] utilisés dans les logs.
Permet de s'assurer que tous les modules utilisent des préfixes cohérents.

Usage :
    from common.log_prefixes import LogPrefixes
    logger.info(f"{LogPrefixes.IMPORT} Table CSKS importée")
"""


class LogPrefixes:
    """Constantes des préfixes [MODULE] utilisés dans les logs AMUE et ECC."""

    # ── AMUE — Blue/Green ────────────────────────────────────────────────────
    BLUEGREEN = '[BLUEGREEN]'
    VIEW_SWITCH = '[VIEW_SWITCH]'
    SYNC = '[SYNC]'
    ROLLBACK = '[ROLLBACK]'

    # ── AMUE — Import pipeline ───────────────────────────────────────────────
    IMPORT = '[IMPORT]'
    INIT = '[INIT]'
    POLLING = '[POLLING]'
    METADATA = '[METADATA]'
    CHECK_SETUP = '[CHECK_SETUP]'

    # ── AMUE — Setup ─────────────────────────────────────────────────────────
    SETUP = '[SETUP]'
    SETUP_REPORT = '[SETUP_REPORT]'
    TABLE_SETUP = '[TABLE_SETUP]'
    TABLE_SETUP_ORCH = '[TABLE_SETUP_ORCH]'

    # ── AMUE — Notifications ─────────────────────────────────────────────────
    NOTIFICATION = '[NOTIFICATION]'
    REPORT = '[REPORT]'
    EMAIL = '[EMAIL]'

    # ── AMUE — Opérateurs pipeline ───────────────────────────────────────────
    STREAM = '[STREAM]'
    BATCH = '[BATCH]'
    DUPLICATE = '[DUPLICATE]'

    # ── AMUE — Services ──────────────────────────────────────────────────────
    ADMIN_STATE = '[ADMIN_STATE]'
    TABLE_CONFIG = '[TABLE_CONFIG]'
    RETRY = '[RETRY]'
    STATUS = '[STATUS]'

    # ── AMUE — Utilitaires ───────────────────────────────────────────────────
    DB = '[DB]'
    SCHEMA = '[SCHEMA]'
    TRANSFORMER = '[TRANSFORMER]'
    TRACING = '[TRACING]'

    # ── ECC ──────────────────────────────────────────────────────────────────
    ECC = '[ECC]'
    ECC_HOOK = '[ECC_HOOK]'
    ECC_IMPORT = '[ECC_IMPORT]'
    ECC_REPORT = '[ECC_REPORT]'
    ECC_METADATA = '[ECC_METADATA]'
