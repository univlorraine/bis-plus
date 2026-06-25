"""
DAG de découverte des tables AMUE (depuis le statut API)

================================================================================
RÔLE
================================================================================

L'API AMUE renvoie, dans son statut ('finish'/'status'), la liste complète des
tables qu'elle met à disposition pour l'université courante. Ce DAG permet
d'ajouter à splus_admin.amue_tables — sans édition SQL manuelle — toutes les
tables vues côté API mais pas encore enregistrées en configuration locale.

================================================================================
UTILISATION
================================================================================

Le dropdown du Param 'tables_to_add' est peuplé en appelant directement le
statut API AMUE au parsing du DAG (mêmes données que discover_tables()) : il
liste les tables exposées par l'API et absentes de splus_admin.amue_tables,
sans nécessiter de run préalable. En cas d'indisponibilité de l'API au
moment du parsing, le dropdown retombe sur le dernier relevé connu (Variable
'amue_discovered_new_tables', écrite à chaque exécution de discover_tables).

  'Trigger DAG w/ config' :
    - 'tables_to_add'        : sélection dans le dropdown des noms à enregistrer
    - ou 'add_all_discovered' : true pour tout enregistrer en une fois
    - 'enabled_default'      : enabled pour les nouvelles entrées (def: true)
    - 'trigger_setup'        : déclenche amue_table_setup si ≥1 table ajoutée
      (calcul des fingerprints/PKs, création de la table physique)

================================================================================
WORKFLOW
================================================================================

    discover_tables()
        ↓  (statut API vs splus_admin.amue_tables)
    register_tables(discovery)
        ↓  (INSERT ON CONFLICT DO NOTHING des tables sélectionnées)
    trigger_setup_if_needed(added)
        ↓  (bool — déclenche TriggerDagRunOperator si ≥1 table ajoutée)
    send_discovery_report(discovery, added, setup_triggered)

================================================================================
"""
import json
import logging

from airflow.models.param import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from amue.infrastructure.notifications import send_failure_notification
from amue.tasks.discovery_dag import (
    discover_tables,
    register_tables,
    send_discovery_report,
    trigger_setup_if_needed,
)
from amue.tasks.discovery_dag.discover_tables import DISCOVERED_TABLES_VARIABLE
from common.dags import DEFAULT_START_DATE, standard_default_args
from common.infrastructure.config.airflow_helpers import AirflowVariableManager as VarMgr

_log = logging.getLogger(__name__)


def _cached_discovered_tables() -> list:
    """Dernier relevé connu (Variable écrite par discover_tables()), pour le repli."""
    try:
        raw = VarMgr.get(DISCOVERED_TABLES_VARIABLE, default=None)
        return json.loads(raw) if raw else []
    except Exception as exc:
        _log.warning(
            f"[DISCOVERY] Lecture Variable '{DISCOVERED_TABLES_VARIABLE}' impossible au parsing: "
            f"{type(exc).__name__}: {exc}"
        )
        return []


def _discovered_tables_choices() -> list:
    """
    Tables exposées par le statut API AMUE et absentes de splus_admin.amue_tables —
    alimente le dropdown du Param 'tables_to_add'.

    Appelle la même API que discover_tables() (statut AMUE), directement au
    parsing du DAG, pour que le dropdown reflète l'état réel de l'API sans
    attendre un run préalable. Protégé par try/except + repli sur le dernier
    relevé connu (Variable DISCOVERED_TABLES_VARIABLE) : si l'API AMUE est
    indisponible au moment du parsing, le parsing du DAG ne doit jamais échouer.
    """
    try:
        from amue.infrastructure.hooks.amue_api_hook import AMUEAPIHook
        from amue.application.api_source_factory import get_status_checker
        from amue.application.table_config_manager import TableConfigManager

        checker = get_status_checker(AMUEAPIHook())
        available = set(checker.get_current_status().keys())
        known = {t['table_name'] for t in TableConfigManager().get_tables_config()}
        new = sorted(available - known)
        VarMgr.set(DISCOVERED_TABLES_VARIABLE, new)
        return new
    except Exception as exc:
        _log.warning(
            f"[DISCOVERY] Appel API AMUE impossible au parsing — repli sur le dernier "
            f"relevé connu ({DISCOVERED_TABLES_VARIABLE}): {type(exc).__name__}: {exc}"
        )
        return _cached_discovered_tables()


_TABLES_TO_ADD_CHOICES = _discovered_tables_choices()


@dag(
    dag_id='amue_table_discovery',
    description="Découverte et enregistrement des tables exposées par le statut API AMUE",

    schedule=None,
    start_date=DEFAULT_START_DATE,
    catchup=False,
    max_active_runs=1,

    tags=['amue', 'discovery'],

    params={
        'tables_to_add': Param(
            default=[],
            type='array',
            examples=_TABLES_TO_ADD_CHOICES,
            description="Tables à enregistrer (sélection multiple, alimentée par le statut "
                         "API AMUE — vide tant qu'aucune table nouvelle n'est détectée). "
                         "Ignoré si 'add_all_discovered' est activé.",
        ),
        'add_all_discovered': Param(
            default=False,
            type='boolean',
            description="Enregistre en une fois toutes les tables vues côté API et "
                        "absentes de splus_admin.amue_tables (ignore 'tables_to_add').",
        ),
        'enabled_default': Param(
            default=True,
            type='boolean',
            description="Valeur de la colonne 'enabled' pour les tables nouvellement enregistrées.",
        ),
        'trigger_setup': Param(
            default=True,
            type='boolean',
            description="Déclenche amue_table_setup après l'enregistrement, si ≥1 table a été ajoutée.",
        ),
    },

    on_failure_callback=send_failure_notification,
    default_args=standard_default_args(),
)
def amue_table_discovery():
    """
    Workflow :
        discover_tables()
            ↓
        register_tables(discovery)
            ↓
        trigger_setup_if_needed(added)   ← retourne bool, pas de DB access
            ↓
        branch_setup_trigger             ← @task.branch
            ├─ trigger_amue_table_setup  ← TriggerDagRunOperator (si True)
            └─ skip_amue_table_setup     ← EmptyOperator (si False)
            ↓ (join, NONE_FAILED_MIN_ONE_SUCCESS)
        send_discovery_report(discovery, added, setup_triggered)
    """
    discovery = discover_tables()
    added = register_tables(discovery)
    setup_triggered = trigger_setup_if_needed(added)

    @task.branch(task_id='branch_setup_trigger')
    def branch_setup_trigger(should_trigger: bool) -> str:
        return 'trigger_amue_table_setup' if should_trigger else 'skip_amue_table_setup'

    branch = branch_setup_trigger(setup_triggered)

    trigger_op = TriggerDagRunOperator(
        task_id='trigger_amue_table_setup',
        trigger_dag_id='amue_table_setup',
        wait_for_completion=False,
    )
    skip_op = EmptyOperator(task_id='skip_amue_table_setup')
    join = EmptyOperator(
        task_id='join_after_trigger',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    branch >> [trigger_op, skip_op] >> join
    report = send_discovery_report(discovery, added, setup_triggered)
    join >> report


amue_table_discovery_dag = amue_table_discovery()
