# Documentation technique — Base intermédiaire SifacPlus

## Stack

| Composant      | Version |
|----------------|---------|
| Apache Airflow | 3.1.7   |
| PostgreSQL     | 15      |
| Python         | 3.12    |
| Docker Compose | —       |
| pytest         | 834 tests |

---

## Structure du projet

```
dags/
├── dag_amue_dynamic_table.py      # DAG principal d'import SIFAC+
├── dag_ecc_dynamic_table.py       # DAG d'import ECC
├── dag_amue_sync.py               # Synchronisation Blue/Green
├── dag_amue_rollback.py           # Rollback Blue/Green
├── dag_amue_status_monitor.py     # Monitoring d'état
└── dag_amue_table_setup.py        # Setup des tables

plugins/common/                    # Socle partagé AMUE + ECC
├── tasks/
│   ├── init_bluegreen.py
│   ├── validate_tables.py
│   ├── prepare_table.py
│   └── switch_views.py
├── operators/batch_inserter.py
├── services/
│   ├── admin_state_manager.py
│   ├── retry_service.py
│   └── bluegreen/
│       ├── bluegreen_manager.py
│       ├── view_switcher.py
│       ├── schema_synchronizer.py
│       └── rollback_manager.py
└── utils/
    ├── config/airflow_helpers.py
    └── database/
        ├── hooks.py
        ├── connection_manager.py
        └── schema_utils.py

plugins/amue/
├── operators/
│   ├── pipeline/
│   │   ├── data_importer.py       # Import paginé
│   │   ├── data_streamer.py       # Streaming API
│   │   ├── batch_inserter.py      # Shim → common
│   │   └── duplicate_detector.py
│   └── table_management/
│       ├── table_filter.py
│       ├── table_manager.py
│       └── table_verifier.py      # Fingerprint
├── services/
│   ├── api/
│   │   ├── polling_service.py
│   │   └── status_checker.py
│   ├── metadata_manager.py
│   ├── table_config_manager.py    # Accès splus_admin.amue_tables
│   └── admin_state_manager.py     # Shim → common
├── tasks/
│   ├── import_dag/                # 9 fonctions @task
│   ├── rollback_dag/
│   └── sync_dag/
├── notifications/
│   ├── email_service.py
│   ├── notifier.py
│   ├── report_generator.py
│   ├── callbacks.py
│   └── templates/
└── utils/
    ├── config/settings.py
    ├── transformers.py            # Conversion types → PostgreSQL
    └── tracing.py

plugins/ecc/
├── hooks/ecc_source_hook.py       # Hook Oracle (stub)
└── tasks/import_dag/

config/
├── airflow_variables.json
├── airflow_connections.json
└── log_config.py

scripts/sql/
├── init_db.sql
├── init_admin_schema.sql
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

| Colonne          | Description                                               |
|------------------|-----------------------------------------------------------|
| `table_name`     | Nom de la table                                           |
| `enabled`        | Active / désactive la table pour l'import                 |
| `primary_key`    | Clés primaires pour UPSERT (prioritaires sur l'API)       |
| `delta`          | Colonne de date pour import différentiel                  |
| `fingerprint_api`| Empreinte structure côté API (auto-générée)              |
| `fingerprint_ul` | Empreinte structure côté PostgreSQL (auto-générée, nom interne) |
| `setup_status`   | `pending` / `ready` / `blocked`                           |
| `ecc_query`      | `NULL` = table SIFAC+ ; non-`NULL` = requête Oracle ECC  |

---

## Principes de conception

- **DAGs = orchestration pure** — toute la logique métier est dans `plugins/`
- **Pas de suppression** — UPSERT uniquement (`INSERT ON CONFLICT UPDATE`)
- **Shims de compatibilité** — les imports `amue.*` continuent de fonctionner via des shims vers `common.*`
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
