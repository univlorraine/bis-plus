"""
DAG de correction ECC — Ré-import manuel de tables Oracle spécifiques

Ce DAG permet de re-importer une sélection de tables ECC sans déclencher
l'import complet planifié. Destiné aux corrections et interventions opérationnelles.

================================================================================
DIFFÉRENCES AVEC dag_ecc_dynamic_table.py
================================================================================

  - schedule=None : déclenchement manuel uniquement
  - Param selected_tables obligatoire : au moins une table doit être sélectionnée
  - Protection sifac_plus conservée (lignes _source='sifac_plus' non écrasées)
  - Synchronisation vers l'actif incluse (sync_ecc_to_active)

================================================================================
UTILISATION
================================================================================

  1. Ouvrir le DAG 'ecc_correction_import' dans l'UI Airflow
  2. Cliquer sur 'Trigger DAG w/ config'
  3. Sélectionner les tables souhaitées dans le champ selected_tables
  4. Soumettre

================================================================================
"""
import logging

from airflow.models.param import Param
from airflow.sdk import dag

from common.dags import DEFAULT_START_DATE, standard_default_args
from ecc.notifications import send_ecc_failure_notification
from ecc.tasks.import_dag import import_data, save_metadata, send_report, sync_to_active
from ecc.tasks.import_dag.select_tables_correction import select_tables_correction
from ecc.utils.config.settings import ECCDefaults
from common.tasks.restore_inactive import restore_inactive

_log = logging.getLogger(__name__)


def _fetch_available_ecc_tables() -> tuple[list[str], list[str]]:
    """Retourne (tables_avec_ecc_query, tables_amue_uniquement) depuis splus_admin.amue_tables."""
    try:
        from common.utils.database.hooks import create_postgres_hook
        hook = create_postgres_hook(schema='splus_admin')
        rows = hook.get_records(
            """
            SELECT table_name,
                   (ecc_query IS NOT NULL AND ecc_query != '') AS has_ecc
            FROM splus_admin.amue_tables
            WHERE enabled = TRUE
            ORDER BY table_name
            """
        )
        with_ecc = [r[0] for r in rows if r[1]]
        amue_only = [r[0] for r in rows if not r[1]]
        return with_ecc, amue_only
    except Exception as exc:
        _log.debug(f"[PARAMS] DB indisponible au parsing du DAG: {exc}")
        return [], []


def _build_ecc_param_description(with_ecc: list[str], amue_only: list[str]) -> str:
    lines = ["Saisir sous forme de tableau JSON : [\"table1\", \"table2\"]"]
    if with_ecc:
        lines.append(f"Avec ecc_query ({len(with_ecc)}) : {', '.join(with_ecc)}")
    if amue_only:
        lines.append(f"AMUE uniquement — pas d'ecc_query ({len(amue_only)}) : {', '.join(amue_only)}")
    if not with_ecc and not amue_only:
        lines.append("Liste indisponible au démarrage — consulter splus_admin.amue_tables.")
    return "\n".join(lines)


_available_ecc_tables, _amue_only_tables = _fetch_available_ecc_tables()
_ECC_PARAM_DESCRIPTION = _build_ecc_param_description(_available_ecc_tables, _amue_only_tables)


@dag(
    dag_id='ecc_correction_import',
    description='Ré-import ECC manuel — sélection de tables Oracle spécifiques',

    schedule=None,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,

    tags=['ecc', 'correction'],

    params={
        'selected_tables': Param(
            default=[],
            type='array',
            description=_ECC_PARAM_DESCRIPTION,
            items={'type': 'string', 'enum': _available_ecc_tables} if _available_ecc_tables else {'type': 'string'},
        ),
    },

    on_failure_callback=send_ecc_failure_notification,
    default_args=standard_default_args(),
)
def ecc_correction_import():
    """
    DAG de correction ECC.

    Workflow :
        select_ecc_tables_correction()            ← filtre conf, schéma inactif
            ↓
        import_ecc_data.expand(table_config=...)  ← Oracle → inactif
            ↙                       ↘
        restore_inactive()       sync_ecc_to_active(imported)   ← inactif → actif
        (ONE_FAILED)             (ALL_SUCCESS)
                                     ↓
                                 save_ecc_metadata.expand()
                                     ↓
                                 send_ecc_report(imported)
    """

    tables = select_tables_correction()
    imported = import_data.expand(table_config=tables)

    restore = restore_inactive(tables=tables, source_name=ECCDefaults.SOURCE_NAME, import_results=imported)

    synced = sync_to_active(imported)
    saved = save_metadata.expand(import_result=imported)
    synced >> saved

    report = send_report(imported)
    saved >> report


ecc_correction_dag = ecc_correction_import()
