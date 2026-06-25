"""
DAG de correction AMUE — Ré-import manuel de tables spécifiques

Ce DAG permet de re-importer une sélection de tables AMUE sans attendre
la fenêtre de polling planifiée. Destiné aux corrections, re-imports partiels
et interventions opérationnelles.

================================================================================
DIFFÉRENCES AVEC dag_amue_dynamic_table.py
================================================================================

  - schedule=None : déclenchement manuel uniquement
  - Pas de sensor (wait_for_api) : appel API direct au moment du run
  - Param selected_tables obligatoire : au moins une table doit être sélectionnée
  - Pas de phase pre/post-import DAGs
  - Import toujours FULL (pas de delta en mode correction)
  - Les tables doivent déjà être en setup_status='ready' (pas de re-setup auto)

================================================================================
UTILISATION
================================================================================

  1. Ouvrir le DAG 'amue_correction_import' dans l'UI Airflow
  2. Cliquer sur 'Trigger DAG w/ config'
  3. Sélectionner les tables souhaitées dans le champ selected_tables
  4. Soumettre

================================================================================
"""
import logging

from airflow.models.param import Param
from airflow.sdk import dag

from amue.infrastructure.notifications import dag_failure_rollback, send_failure_notification
from amue.tasks.import_dag import (
    check_setup_status,
    import_data,
    init_bluegreen,
    save_metadata,
    send_report,
    switch_views,
)
from amue.tasks.import_dag.select_tables_correction import select_tables_correction
from common.domain.protected_source import PROTECTED_SOURCE
from common.dags import DEFAULT_START_DATE, standard_default_args
from common.tasks.restore_inactive import restore_inactive

_log = logging.getLogger(__name__)


def _fetch_available_tables() -> tuple[list[str], list[str]]:
    """Retourne (tables_activées, tables_désactivées) depuis splus_admin.amue_tables."""
    try:
        from common.infrastructure.database.hooks import create_postgres_hook
        hook = create_postgres_hook(schema='splus_admin')
        rows = hook.get_records(
            "SELECT table_name, enabled FROM splus_admin.amue_tables ORDER BY table_name"
        )
        enabled = [r[0] for r in rows if r[1]]
        disabled = [r[0] for r in rows if not r[1]]
        return enabled, disabled
    except Exception as exc:
        _log.warning(f"[PARAMS] DB indisponible au parsing du DAG: {type(exc).__name__}: {exc}")
        return [], []


def _build_param_description(enabled: list[str], disabled: list[str]) -> str:
    lines = ["Saisir sous forme de tableau JSON : [\"table1\", \"table2\"] — import FULL uniquement."]
    if enabled:
        lines.append(f"Actives ({len(enabled)}) : {', '.join(enabled)}")
    if disabled:
        lines.append(f"Désactivées ({len(disabled)}) : {', '.join(disabled)}")
    if not enabled and not disabled:
        lines.append("Liste indisponible au démarrage — consulter splus_admin.amue_tables.")
    return "\n".join(lines)


_available_tables, _disabled_tables = _fetch_available_tables()
_PARAM_DESCRIPTION = _build_param_description(_available_tables, _disabled_tables)


@dag(
    dag_id='amue_correction_import',
    description='Ré-import AMUE manuel — sélection de tables spécifiques',

    schedule=None,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'correction'],

    params={
        'selected_tables': Param(
            default=[],
            type='array',
            description=_PARAM_DESCRIPTION,
            examples=_available_tables,
        ),
    },

    on_failure_callback=dag_failure_rollback,
    default_args=standard_default_args(
        on_failure_callback=send_failure_notification,
    ),
)
def amue_correction_import():
    """
    DAG de correction AMUE.

    Workflow :
        init_bluegreen()
            ↓
        select_tables_correction(bluegreen_ctx)   ← API directe + filtre conf
            ↓
        check_setup_status(tables)                ← STOPPE si pending/blocked
            ↓
        import_data.expand(table_info=checked)    ← FULL uniquement
            ↓
        save_metadata(imported) → switch_views() → send_report()
    """

    bluegreen_ctx = init_bluegreen()
    tables = select_tables_correction(bluegreen_ctx)
    checked = check_setup_status(tables)
    imported = import_data.expand(table_info=checked)

    _ = restore_inactive(tables=checked, source_name=PROTECTED_SOURCE, import_results=imported)

    metadata = save_metadata(imported, {})
    switch_result = switch_views(metadata)
    send_report(imported, switch_result, {})


amue_correction_dag = amue_correction_import()
