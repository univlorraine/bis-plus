# Documentation technique — Base intermédiaire SifacPlus

## Stack

| Composant      | Version |
|----------------|---------|
| Apache Airflow | 3.1.7   |
| PostgreSQL     | 15      |
| Python         | 3.12    |
| Docker Compose | —       |
| pytest         | 857 tests |

---

## Structure du projet

```
dags/
├── dag_amue_dynamic_table.py      # DAG principal d'import SIFAC+ (dag_id: amue_multi_table_import)
├── dag_ecc_dynamic_table.py       # DAG d'import ECC (dag_id: ecc_multi_table_import)
├── dag_amue_sync.py               # Synchronisation Blue/Green (dag_id: amue_sync_schemas)
├── dag_amue_rollback.py           # Rollback Blue/Green (dag_id: amue_rollback)
├── dag_amue_refresh_views.py      # Rafraîchissement des vues (dag_id: amue_refresh_views)
├── dag_amue_status_monitor.py     # Monitoring d'état (dag_id: amue_status_monitor)
└── dag_amue_table_setup.py        # Setup des tables (dag_id: amue_table_setup)

plugins/common/                    # Socle partagé AMUE + ECC
├── tasks/
│   └── restore_inactive.py        # @task partagée (modèle de mutualisation)
├── operators/
│   └── batch_inserter.py          # BatchInserter (utilisé par AMUE et ECC)
├── services/
│   ├── admin_state_manager.py     # AdminStateManager
│   ├── retry_service.py
│   └── bluegreen/
│       ├── bluegreen_manager.py
│       ├── bluegreen_lock_manager.py
│       ├── bluegreen_schema_resolver.py
│       ├── bluegreen_state_manager.py
│       ├── view_switcher.py
│       └── schema_synchronizer.py
├── notifications/
│   ├── base_notifier.py
│   ├── callbacks_utils.py
│   └── email_service.py
└── utils/
    ├── config/airflow_helpers.py
    └── database/
        ├── hooks.py
        ├── connection_manager.py
        └── schema_utils.py

plugins/amue/
├── hooks/amue_api_hook.py
├── exceptions/                    # AMUEError, AMUEAPIError, etc.
├── operators/
│   ├── pipeline/
│   │   ├── data_importer.py       # AMUEDataImporter (orchestration)
│   │   ├── data_streamer.py       # Streaming API
│   │   ├── duplicate_detector.py
│   │   └── import_config_validator.py
│   └── table_management/
│       ├── table_filter.py        # AMUETableFilter
│       ├── table_manager.py       # AMUETableManager
│       └── table_verifier.py      # AMUETableVerifier (fingerprint)
├── services/
│   ├── api/
│   │   ├── polling_service.py
│   │   ├── status_checker.py
│   │   ├── status_monitor.py
│   │   └── ...
│   ├── metadata_manager.py
│   ├── table_config_manager.py    # Accès splus_admin.amue_tables
│   └── table_setup_orchestrator.py
├── sensors/amue_api_sensor.py
├── tasks/
│   ├── import_dag/                # @task : check_setup_status, polling, import_data, ...
│   ├── refresh_views_dag/
│   ├── rollback_dag/
│   ├── setup_dag/
│   └── sync_dag/
├── notifications/
│   ├── notifier.py
│   ├── callbacks.py
│   ├── report_generator.py
│   └── templates*.py
├── types_amue.py                  # TableInfo, ImportResult, ...
└── utils/
    ├── config/settings.py         # AMUEConfig, AMUEDefaults
    ├── transformers.py            # Conversion types → PostgreSQL
    └── tracing.py

plugins/ecc/
├── hooks/ecc_source_hook.py       # Hook source ECC
├── notifications/
│   ├── ecc_notifier.py
│   ├── ecc_callbacks.py
│   └── ecc_templates.py
├── tasks/import_dag/              # @task : select_tables, import_data, sync_to_active, ...
└── utils/config/settings.py       # ECCConfig, ECCDefaults

config/
├── airflow_variables.json
├── airflow_connections.json
└── log_config.py

scripts/sql/
├── init_db.sql                    # Crée schémas, tables splus_admin, permissions
└── custom_views/                  # Vues métier personnalisées

tests/                             # 834 tests (pytest)
```

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

> 📷 **Capture d'écran suggérée** : *Arborescence dans DBeaver ou pgAdmin listant les 4 schémas avec leurs tables et vues, montrant par exemple `splus.csks` (vue) pointant vers `splus_blue.csks` (table).*

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

### Table `splus_admin.amue_tables`

| Colonne          | Description                                                |
|------------------|------------------------------------------------------------|
| `table_name`     | Nom de la table                                            |
| `enabled`        | Active / désactive la table pour l'import                  |
| `primary_key`    | Clés primaires pour UPSERT (prioritaires sur l'API)        |
| `delta`          | Colonne de date pour import différentiel                   |
| `fingerprint_api`| Empreinte structure côté API (auto-générée)                |
| `fingerprint_local` | Empreinte structure côté PostgreSQL (auto-générée, nom interne) |
| `setup_status`   | `pending` / `ready` / `blocked`                            |
| `ecc_query`      | `NULL` = Pas de requête ECC <br/> non-`NULL` = requête ECC |

---

## Mapping de types (source → PostgreSQL)

Les données reçues de l'API AMUE (format SQLite/SAP) sont converties en types PostgreSQL avant insertion. Le mapping est configurable via la variable Airflow `TYPE_MAPPING_SQLITE_TO_POSTGRES` :

| Type source | Type PostgreSQL |
|-------------|-----------------|
| `TEXT`, `CLOB` | `TEXT` |
| `VARCHAR`, `NVARCHAR` | `VARCHAR` |
| `CHAR`, `CHARACTER`, `NCHAR` | `BPCHAR` |
| `INTEGER`, `MEDIUMINT` | `INTEGER` |
| `TINYINT`, `SMALLINT`, `INT2` | `SMALLINT` |
| `BIGINT`, `INT8` | `BIGINT` |
| `NUMERIC`, `DECIMAL` | `NUMERIC` |
| `REAL`, `DOUBLE`, `FLOAT` | `DOUBLE PRECISION` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE`, `DATETIME`, `TIMESTAMP` | `TIMESTAMP` |
| `BLOB` | `BYTEA` |

La conversion est appliquée par `plugins/amue/utils/transformers.py` lors de chaque batch d'import.

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
| `amue_pre_import_dags` | `[]` | DAGs déclenchés avant import |
| `amue_post_import_dags` | `[]` | DAGs déclenchés après import |
| `amue_report_recipients` | — | Destinataires email |

Importer toutes les variables en une commande :
```bash
airflow variables import config/airflow_variables.json
```

---

## Principes de conception

- **DAGs = orchestration pure** — toute la logique métier est dans `plugins/`
- **Pas de suppression** — UPSERT uniquement (`INSERT ON CONFLICT UPDATE`)
- **Plugins comme paires de sœurs** — `amue/` et `ecc/` sont deux plugins indépendants qui partagent `common/`. Aucun import croisé `ecc → amue`.
- **Surface publique minimale** — `from amue import …` n'expose que types, exceptions et config ; tout le reste est importé depuis son chemin canonique (`from amue.operators.pipeline.data_importer import AMUEDataImporter`).
- **Switch atomique** — les vues sont recréées via `DROP + CREATE` dans une seule transaction
- **Verrou PostgreSQL** — empêche deux imports simultanés sans race condition
- **Double fingerprint** — détecte les changements de structure avant d'importer

---

## Tests

```bash
pytest tests/ -v
pytest --cov=plugins/amue --cov-report=html
```

Les tests couvrent les opérateurs, services, Blue/Green, notifications, retry, et transformations. Ils utilisent `pythonpath = plugins` (configuré dans `pytest.ini`) — pas de hack `sys.path`.
