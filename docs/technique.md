# Documentation technique — Base intermédiaire SifacPlus

## Stack

| Composant      | Version   |
|----------------|-----------|
| Apache Airflow | 3.1.7     |
| PostgreSQL     | 15        |
| Python         | 3.12      |
| Docker Compose | —         |
| pytest         | 891 tests |

---

## Structure du projet

```
dags/
├── dag_amue_dynamic_table.py      # DAG principal d'import SIFAC+ (dag_id: amue_multi_table_import)
├── dag_amue_correction.py         # Ré-import AMUE manuel (dag_id: amue_correction_import)
├── dag_amue_table_setup.py        # Setup tables + fingerprints (dag_id: amue_table_setup)
├── dag_amue_table_discovery.py    # Découverte tables API non enregistrées (dag_id: amue_table_discovery)
├── dag_amue_settings.py           # Formulaire variables Airflow (dag_id: amue_settings)
├── dag_amue_sync.py               # Synchronisation Blue/Green (dag_id: amue_sync_schemas)
├── dag_amue_rollback.py           # Rollback Blue/Green (dag_id: amue_rollback)
├── dag_amue_refresh_views.py      # Rafraîchissement des vues (dag_id: amue_refresh_views)
├── dag_amue_status_monitor.py     # Monitoring d'état API (dag_id: amue_status_monitor)
├── dag_ecc_dynamic_table.py       # DAG d'import ECC (dag_id: ecc_multi_table_import)
└── dag_ecc_correction.py          # Ré-import ECC manuel (dag_id: ecc_correction_import)

plugins/common/                    # Socle partagé AMUE + ECC
├── domain/
│   ├── exceptions.py              # BaseError, BatchError, DataError, DatabaseError, SchemaError,
│   │                              #   BlueGreenError, ConcurrentImportError, ViewSwitchError
│   ├── fingerprint.py             # compute_structure_hash_with_pk, compare_fingerprints
│   ├── interfaces.py              # Protocols : SqlExecutor, ConnectionProvider, StateStore
│   ├── protected_source.py        # PROTECTED_SOURCE = 'sifac_plus'
│   ├── state_types.py             # Types de l'état Blue/Green
│   └── validators.py              # validate_table_name, validate_column_name, validate_identifier
├── application/
│   ├── admin_state_manager.py     # AdminStateManager (accès splus_admin.amue_state)
│   ├── batch_upserter.py          # BatchUpserter (UPSERT par lots, utilisé par AMUE et ECC)
│   ├── bluegreen/
│   │   ├── bluegreen_manager.py   # Façade Blue/Green (get_active_schema, try_acquire_import_lock)
│   │   ├── bluegreen_lock_manager.py
│   │   ├── bluegreen_schema_resolver.py
│   │   ├── bluegreen_state_manager.py
│   │   ├── schema_synchronizer.py # Sync actif → inactif (DAG amue_sync_schemas)
│   │   └── view_switcher.py       # DROP + CREATE vues splus.* en transaction unique
│   ├── duplicate_detector.py      # DuplicateDetector
│   ├── retry_service.py           # RetryService (backoff exponentiel)
│   └── table_creator.py           # Création DDL des tables dans le schéma cible
├── infrastructure/
│   ├── config/
│   │   ├── airflow_helpers.py     # AirflowVariableManager (get, get_int, get_required, set)
│   │   └── recipients.py          # parse_recipients (CSV → liste d'adresses email)
│   ├── database/
│   │   ├── connection_manager.py  # PostgresConnectionManager (cycle de vie connexion)
│   │   ├── hooks.py               # create_postgres_hook / resolve_postgres_hook
│   │   ├── identifier_qualifier.py# Qualification sécurisée des noms schema.table
│   │   ├── schema_introspection.py# Lecture information_schema (colonnes, vues)
│   │   └── sql_file_loader.py     # Chargement + substitution de fichiers .sql
│   ├── notifications/
│   │   ├── base_notifier.py       # BaseNotifier (interface d'envoi email)
│   │   ├── base_templates.py      # BaseTemplates (CSS + helpers HTML partagés)
│   │   ├── email_service.py       # EmailService (SMTP)
│   │   └── failure_callback_helpers.py # on_failure_callback partagé
│   └── observability/
│       ├── log_prefixes.py        # Constantes de préfixes de log ([IMPORT], [BATCH], ...)
│       ├── logging_context.py     # Context manager enrichissant les logs
│       ├── structured_logging.py  # Helpers de log structuré
│       └── tracing.py             # generate_correlation_id, MemoryTracker,
│                                  #   OperationTimer, TracingContext, to_iso_str
├── tasks/
│   ├── import_summary.py          # summarize_import_results (@task partagée)
│   └── restore_inactive.py        # restore_inactive (@task partagée)
└── dags/
    └── dag_defaults.py            # DEFAULT_START_DATE, standard_default_args

plugins/amue/
├── domain/
│   ├── exceptions/                # AMUEError, AMUEAPIError, AMUEAuthError,
│   │                              #   AMUENetworkError, AMUEDataError, AMUEDatabaseError,
│   │                              #   AMUEBatchError, ... (héritent de common.domain.exceptions)
│   ├── entrepot_structure_fetcher.py  # EntrepotStructureFetcher (source "entrepôt")
│   ├── fingerprint_comparator.py  # compute_diff, format_pg_type, compare_fingerprints
│   ├── finish_timestamp_validator.py  # Validation du timestamp de fin de rapport
│   ├── polling_strategy_calculator.py # Calcul de l'intervalle de polling (backoff)
│   ├── structure_fetcher.py       # APIStructureFetcher (source CDV)
│   ├── transformers.py            # parse_column_definition (SQLite/SAP → PostgreSQL)
│   └── types_amue.py             # TableInfo, ImportResult, PollingResult, ...
├── application/
│   ├── api_source_factory.py      # Factory CDV/entrepôt (dispatch via amue_api_source)
│   ├── metadata_manager.py        # Sauvegarde des métadonnées d'import (amue_state)
│   ├── pipeline/
│   │   ├── data_import_pipeline.py    # Pipeline complet (stream + upsert + retry)
│   │   ├── data_importer.py           # AMUEDataImporter (orchestration d'une table)
│   │   └── import_config_validator.py # Validation de la configuration d'import
│   ├── polling_service.py         # PollingService (attente rapport API)
│   ├── status_monitor.py          # StatusMonitor (surveillance API amue_status_monitor)
│   ├── table_config_manager.py    # TableConfigManager (accès splus_admin.amue_tables)
│   ├── table_management/
│   │   ├── table_filter.py        # AMUETableFilter (FULL vs DELTA, fail-fast)
│   │   ├── table_manager.py       # AMUETableManager (création DDL + fingerprints)
│   │   └── table_verifier.py      # AMUETableVerifier (orchestration setup)
│   └── table_setup_orchestrator.py    # Orchestration complète du setup
├── infrastructure/
│   ├── api/
│   │   ├── data_streamer.py           # AMUEDataStreamer (pagination CDV)
│   │   ├── entrepot_data_streamer.py  # EntrepotDataStreamer (pagination entrepôt)
│   │   ├── entrepot_status_checker.py # EntrepotStatusChecker (statut entrepôt)
│   │   └── status_checker.py          # AMUEStatusChecker (statut CDV)
│   ├── config/settings.py         # AMUEConfig (dataclass validée), AMUEDefaults
│   ├── hooks/amue_api_hook.py     # AMUEAPIHook (OAuth2 client_credentials)
│   ├── notifications/
│   │   ├── callbacks.py           # on_failure_callback AMUE
│   │   ├── notifier.py            # AMUENotifier
│   │   ├── report_generator.py    # Génération HTML des rapports d'import
│   │   └── templates*.py          # Templates HTML (import, setup, sync, rollback, ...)
│   └── sensors/amue_api_sensor.py # AMUEAPISensor (mode reschedule)
└── tasks/
    ├── discovery_dag/             # discover_tables, register_tables,
    │                              #   trigger_setup_if_needed, send_discovery_report
    ├── import_dag/                # init_bluegreen, polling, select_tables (correction),
    │                              #   check_setup_status, import_data, save_metadata,
    │                              #   switch_views, send_report
    ├── refresh_views_dag/         # detect_active_schema, refresh_custom_views, send_refresh_report
    ├── rollback_dag/              # check_rollback, perform_rollback, send_rollback_report
    ├── setup_dag/                 # select_setup_tables, setup_table, send_setup_report
    └── sync_dag/                  # init_sync, run_sync, send_sync_report

plugins/ecc/
├── application/
│   └── ecc_data_importer.py       # ECCDataImporter (Oracle → PostgreSQL inactif)
├── infrastructure/
│   ├── config/settings.py         # ECCConfig, ECCDefaults
│   ├── hooks/ecc_source_hook.py   # ECCSourceHook (ODBC Oracle / SQL Server)
│   └── notifications/             # ecc_notifier, ecc_callbacks, ecc_templates
└── tasks/import_dag/              # select_tables, select_tables_correction,
                                   #   import_data, sync_to_active, save_metadata, send_report

config/
├── airflow_variables.json         # Variables Airflow (import en masse)
├── airflow_connections.json       # Structure des connexions (sans credentials)
└── log_config.py

scripts/sql/
├── init_db.sql                    # Crée schémas, tables splus_admin, permissions
├── migrations/                    # Migrations SQL applicatives (convention NNNN_description.sql)
└── custom_views/                  # Vues métier personnalisées (template {target_schema})

tests/                             # 891 tests (pytest)
```

---

## Architecture en couches (DDD)

Le code des plugins suit un découpage **Domain / Application / Infrastructure** :

| Couche | Règle d'import | Rôle |
|--------|---------------|------|
| `domain/` | **Zéro** import `airflow.*`, `psycopg2.*`, `requests` | Logique métier pure — types, exceptions, transformations, règles |
| `application/` | Compose `domain/` + interfaces injectées | Orchestration des cas d'usage — pas d'accès direct aux frameworks |
| `infrastructure/` | Tout import externe autorisé | Adaptateurs concrets : hooks Airflow, requêtes psycopg2, SMTP, API REST |

Les DAGs (`dags/*.py`) restent fins et n'importent que depuis `tasks/`.

Les `Protocol` de `common/domain/interfaces.py` (`SqlExecutor`, `ConnectionProvider`,
`StateStore`) permettent à la couche `application/` d'être testée sans mock Airflow.

---

## Schémas PostgreSQL

```
splus_blue        Tables Blue (données)
splus_green       Tables Green (données)
splus             Vues publiques → pointent vers Blue OU Green
splus_admin       Administration
  ├── amue_state  État global (singleton, id=1)
  └── amue_tables Configuration et fingerprints par table
```

### Table `splus_admin.amue_state`

| Colonne                | Description                                        |
|------------------------|----------------------------------------------------|
| `active_schema`        | `blue` ou `green` (audit — source de vérité : `information_schema.views`) |
| `last_finish_timestamp`| Timestamp du dernier rapport SIFAC+ traité         |
| `last_report_start`    | Référence delta (timestamp `start` de l'API)       |
| `last_successful_run`  | Horodatage du dernier import réussi                |
| `import_in_progress`   | Verrou d'import                                    |
| `last_switch_timestamp`| Dernier switch Blue/Green                          |
| `last_sync_timestamp`  | Dernière synchronisation Blue/Green                |
| `import_started_at`    | Timestamp du début de l'import en cours            |
| `import_correlation_id`| ID de corrélation pour la traçabilité des logs     |
| `updated_at`           | Horodatage de la dernière modification de la ligne |

### Table `splus_admin.amue_tables`

| Colonne          | Description                                                |
|------------------|------------------------------------------------------------|
| `table_name`     | Nom de la table                                            |
| `enabled`        | Active / désactive la table pour l'import                  |
| `primary_key`    | Clés primaires pour UPSERT (prioritaires sur l'API)        |
| `delta`          | Colonne de date pour import différentiel                   |
| `fingerprint_api`| Empreinte structure côté API (auto-générée)                |
| `fingerprint_local` | Empreinte structure côté PostgreSQL (auto-générée)      |
| `setup_status`   | `pending` / `ready` / `blocked`                            |
| `ecc_query`      | `NULL` = table AMUE ; non-`NULL` = requête ECC Oracle      |

---

## Mapping de types (source → PostgreSQL)

Les données reçues de l'API AMUE (format SQLite/SAP) sont converties en types PostgreSQL avant insertion. Le mapping est configurable via la variable Airflow `TYPE_MAPPING_SQLITE_TO_POSTGRES` :

| Type source | Type PostgreSQL |
|-------------|-----------------|
| `TEXT`, `CLOB` | `TEXT` |
| `VARCHAR`, `NVARCHAR` | `VARCHAR` |
| `CHAR`, `CHARACTER`, `NCHAR` | `CHAR` |
| `INTEGER`, `MEDIUMINT` | `INTEGER` |
| `TINYINT`, `SMALLINT`, `INT2` | `SMALLINT` |
| `BIGINT`, `INT8` | `BIGINT` |
| `NUMERIC`, `DECIMAL` | `NUMERIC` |
| `REAL`, `DOUBLE`, `FLOAT` | `DOUBLE PRECISION` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE`, `DATETIME`, `TIMESTAMP` | `TIMESTAMP` |
| `BLOB` | `BYTEA` |

La conversion est appliquée par `parse_column_definition()` dans `plugins/amue/domain/transformers.py`.

---

## Variables Airflow

La liste complète des variables avec leurs valeurs par défaut est dans `config/airflow_variables.json`. Les principales :

| Variable | Défaut | Rôle |
|----------|--------|------|
| `amue_import_schedule` | `0 2 * * *` | Cron import principal |
| `amue_polling_interval_minutes` | `10` | Fréquence sensor (minutes) |
| `amue_max_wait_hours` | `6` | Timeout sensor (heures) |
| `amue_import_batch_size` | `5000` | Lignes par batch API |
| `amue_import_parallel_workers` | `1` | Tables en parallèle |
| `amue_force_import` | `false` | Bypass sensor (dev uniquement) |
| `amue_api_source` | `entrepot` | Source API active : `cdv` ou `entrepot` |
| `api_endpoint_entrepot` | — | Endpoint base source "entrepôt" (avec `${univ}`) |
| `amue_pre_import_dags` | `[]` | DAGs déclenchés avant import |
| `amue_post_import_dags` | `[]` | DAGs déclenchés après import |
| `amue_tables_to_purge` | `[]` | Tables purgées (TRUNCATE) avant import (JSON array) |
| `amue_report_recipients` | — | Destinataires email |

Importer toutes les variables en une commande :
```bash
airflow variables import config/airflow_variables.json
```

---

## Principes de conception

- **DAGs = orchestration pure** — toute la logique métier est dans `plugins/*/application/` et `plugins/*/domain/`
- **Pas de suppression** — UPSERT uniquement (`INSERT ON CONFLICT UPDATE`)
- **Plugins comme paires de sœurs** — `amue/` et `ecc/` sont deux plugins indépendants qui partagent `common/`. Aucun import croisé `ecc → amue`.
- **Surface publique minimale** — `from amue import …` n'expose que types, exceptions et config ; tout le reste est importé depuis son chemin canonique (`from amue.application.pipeline.data_importer import AMUEDataImporter`)
- **Switch atomique** — les vues sont recréées via `DROP + CREATE` dans une seule transaction
- **Verrou PostgreSQL** — empêche deux imports simultanés sans race condition (`UPDATE ... RETURNING id`)
- **Double fingerprint** — détecte les changements de structure avant d'importer
- **Factory de source API** — `amue.application.api_source_factory` dispatche vers CDV ou entrepôt selon `amue_api_source`

---

## Tests

```bash
pytest tests/ -v
pytest --cov=plugins/amue --cov-report=html
```

Les tests couvrent les opérateurs, services, Blue/Green, notifications, retry, et transformations. Ils utilisent `pythonpath = plugins` (configuré dans `pytest.ini`) — pas de hack `sys.path`.

La structure de test miroir la structure source :
- `tests/amue/application/` ↔ `plugins/amue/application/`
- `tests/common/application/` ↔ `plugins/common/application/`
- `tests/amue/domain/` ↔ `plugins/amue/domain/`
