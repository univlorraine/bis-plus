# Projet AMUE Import - Présentation Technique

## Vue d'ensemble

Ce projet orchestre l'import de données financières universitaires vers PostgreSQL via Apache Airflow. Il couvre deux sources :

- **AMUE (SIFAC+)** — données financières issues de l'API AMUE, import quotidien avec architecture Blue/Green et détection de changements de structure
- **Oracle ECC** — tables SAP complémentaires importées depuis une base Oracle, avec protection des données SIFAC+

L'architecture Blue/Green garantit des bascules atomiques et un rollback instantané : les vues publiques (`splus.*`) pointent toujours vers un schéma complet et cohérent.

---

## Architecture du Workflow AMUE

### DAG principal : `amue_multi_table_import`

```
PHASE 0 — BLUE/GREEN INIT
    init_bluegreen()
        • Détermine le schéma cible (opposé de l'actif)
        • Acquiert le verrou d'import (atomique via PostgreSQL)
               |
PHASE 1 — POLLING
    AMUEAPISensor (wait_for_api)
        • Attend que l'API AMUE publie un nouveau rapport
        • Mode reschedule : libère le worker entre les tentatives
        • Pousse polling_result en XCom
               |
    select_tables(bluegreen_ctx)
        • Charge la configuration depuis splus_admin.amue_tables
        • Valide que chaque table configurée existe dans le statut API (fail-fast)
        • Détermine le type d'import : FULL ou DELTA
               |
PHASE 2 — SETUP (délégué à amue_table_setup)
    TriggerDagRunOperator → amue_table_setup
        • Crée les tables absentes dans le schéma cible
        • Calcule et sauvegarde les fingerprints (structure API + structure PG)
        • Passe en blocked si la structure a changé
               |
PHASE 3 — VALIDATION ET IMPORT
    check_setup_status(tables)
        • Lit setup_status depuis splus_admin.amue_tables
        • Fail-fast si une table est pending ou blocked

    import_data.expand(table_info=checked)   [1 task par table, parallèle]
        • Récupère les données par batch depuis l'API
        • UPSERT via INSERT ON CONFLICT (colonnes lues depuis information_schema)
        • Retry automatique sur erreurs réseau
               |
PHASE 4 — FINALISATION
    save_metadata(imported)
        • Sauvegarde finish_timestamp, report_start, last_successful_run
        • Retourne le contexte pour le switch Blue/Green

    switch_views(metadata)
        • Recrée toutes les vues splus.* en DROP + CREATE (transaction unique)
        • Bascule atomique : soit toutes basculent, soit aucune

    send_report(imported, switch_result)
        • Génère un rapport HTML avec statistiques par table
        • Envoie par email aux destinataires configurés
```

**Planification :** configurable via `amue_import_schedule` (défaut `0 2 * * *`).

---

## Architecture du Workflow ECC

### DAG principal : `ecc_multi_table_import`

```
PHASE 1 — SÉLECTION
    select_ecc_tables()
        • Lit splus_admin.amue_tables (tables enabled avec ecc_query non-NULL)
        • Détermine le schéma inactif via BlueGreenManager
               |
PHASE 2 — IMPORT PARALLÈLE (schéma inactif)
    import_ecc_data.expand(table_config=tables)   [1 task par table]
        • Exécute la ecc_query Oracle via ECCSourceHook
        • UPSERT dans le schéma inactif
        • Protection sifac_plus : DO UPDATE SET ... WHERE _source != 'sifac_plus'
               |
PHASE 3 — SYNCHRONISATION VERS L'ACTIF
    sync_ecc_to_active(imported)
        • UPSERT inactif → actif (lignes ECC uniquement, transaction unique)
        • Si erreur : schéma actif inchangé
               |
PHASE 4 — RAPPORT
    save_ecc_metadata + send_ecc_report
```

**Planification :** configurable via `ecc_import_schedule` (défaut `0 4 * * *`).

---

## DAGs de correction manuelle

### `amue_correction_import` et `ecc_correction_import`

Ces DAGs permettent de ré-importer une sélection de tables sans attendre l'heure planifiée, pour les corrections et interventions opérationnelles.

**Différences par rapport aux DAGs principaux :**

| Aspect | DAG principal | DAG correction |
|--------|---------------|----------------|
| Schedule | cron configuré | `None` (manuel uniquement) |
| Sensor API | AMUEAPISensor (attend nouveau rapport) | Pas de sensor (appel direct) |
| Sélection tables | toutes les tables activées | `selected_tables` dans `dag_run.conf` (obligatoire) |
| Type d'import | FULL ou DELTA | FULL uniquement |
| Pre/post DAGs | configurables | aucun |

**Utilisation :**

1. Ouvrir le DAG `amue_correction_import` ou `ecc_correction_import` dans l'UI
2. Cliquer sur **Trigger DAG w/ config**
3. Dans le champ `selected_tables`, saisir le tableau JSON des tables : `["CSKS", "LFA1"]`
4. Soumettre

Les tables doivent déjà avoir `setup_status = 'ready'` dans `splus_admin.amue_tables`.

---

## Autres DAGs

| DAG | dag_id | Schedule | Rôle |
|-----|--------|----------|------|
| `dag_amue_table_setup.py` | `amue_table_setup` | Manuel / déclenché par import | Création tables, calcul fingerprints, détection changements de structure |
| `dag_amue_rollback.py` | `amue_rollback` | Manuel uniquement | Restaure le schéma `_offline` vers l'actif |
| `dag_amue_sync.py` | `amue_sync_schemas` | `amue_sync_schedule` (`0 6 * * *`) | Copie actif → inactif pour les maintenir à parité |
| `dag_amue_refresh_views.py` | `amue_refresh_views` | Manuel uniquement | Recrée les vues custom depuis `scripts/sql/custom_views/` |
| `dag_amue_status_monitor.py` | `amue_status_monitor` | `amue_monitor_schedule` (`0 22 * * *`) | Surveille l'API AMUE chaque minute pendant 4h, log les changements |

---

## Description des composants principaux

### `AMUETableFilter` (`plugins/amue/operators/table_management/table_filter.py`)

Filtre les tables configurées dans `splus_admin.amue_tables` :

1. Charge la liste depuis la BDD
2. Sépare les tables activées (`enabled=TRUE`) des désactivées
3. Vérifie que chaque table activée existe dans le statut API (fail-fast si absente)
4. Détermine FULL vs DELTA selon la présence d'une colonne delta et d'un `last_report_start`

### `AMUEDataImporter` (`plugins/amue/operators/pipeline/data_importer.py`)

Orchestre l'import d'une table :

1. `DataStreamer` : récupère les données page par page depuis l'API (générateur)
2. `BatchInserter` : UPSERT par lots de `amue_import_batch_size` lignes (défaut 5 000)
3. Retry automatique sur erreurs réseau via `RetryService`

```sql
-- UPSERT (si clés primaires configurées)
INSERT INTO splus_blue.csks (col1, col2, ...)
VALUES (...)
ON CONFLICT (kokrs, kostl, datbi) DO UPDATE SET col1 = EXCLUDED.col1, ...
```

### `BlueGreenManager` (`plugins/common/services/bluegreen/bluegreen_manager.py`)

Façade pour la gestion Blue/Green :

- `get_active_schema()` — lit `information_schema.views` (source de vérité)
- `get_target_schema()` — schéma opposé à l'actif
- `try_acquire_import_lock()` — `UPDATE ... WHERE import_in_progress = FALSE RETURNING id` (atomique, sans race condition)

### `AdminStateManager` (`plugins/common/services/admin_state_manager.py`)

Accès à `splus_admin.amue_state` (singleton `id = 1`) :

- Timestamps : `last_finish_timestamp`, `last_report_start`, `last_successful_run`
- Verrou d'import : `import_in_progress`, `import_started_at`, `import_correlation_id`
- Switch Blue/Green : `active_schema`, `last_switch_timestamp`

---

## Démonstrations des cas d'échec

Un script interactif permet de simuler les principaux cas d'erreur :

```bash
./scripts/dev/demo_failures.sh
```

Options disponibles :

| Option | Scénario simulé |
|--------|-----------------|
| `1` | Table absente de l'API |
| `2` | API indisponible (timeout simulé) |
| `3` | Logs en temps réel |
| `4` | Ouvrir MailHog |
| `5` | Ouvrir Airflow UI |
| `6` | Restaurer la configuration normale |

---

## Script de gestion : manage.sh

Point d'entrée unique pour toutes les opérations du projet :

```bash
./manage.sh [COMMANDE] [ARGUMENTS]
./manage.sh help          # Liste toutes les commandes disponibles
```

### Commandes par catégorie

**Services :**

| Commande | Description |
|----------|-------------|
| `start` | Démarre tous les conteneurs Docker |
| `stop` | Arrête tous les conteneurs |
| `restart` | Redémarre tous les conteneurs |
| `status` | État des services |
| `logs [service]` | Logs (tous les services ou un seul) |

**Configuration :**

| Commande | Description |
|----------|-------------|
| `setup` | Installation complète interactrice |
| `config` | Reconfigure variables et connexions |
| `auto-fix` | Correction automatique complète |
| `verify` | Vérifie la configuration actuelle |
| `diagnose` | Diagnostic système complet |

**DAGs :**

| Commande | Description |
|----------|-------------|
| `dags` | Liste tous les DAGs |
| `trigger <dag_id>` | Déclenche un DAG |
| `pause / unpause <dag_id>` | Active ou désactive un DAG |

**Tables :**

| Commande | Description |
|----------|-------------|
| `list-tables` | Affiche la configuration des tables |
| `add-table <nom>` | Ajoute une table |
| `enable-table <nom>` / `disable-table <nom>` | Active ou désactive une table |
| `remove-table <nom>` | Supprime une table de la configuration |

**Base de données :**

| Commande | Description |
|----------|-------------|
| `db-shell` | Shell PostgreSQL |
| `db-backup` | Sauvegarde horodatée |
| `db-restore <fichier>` | Restauration |

**Développement :**

| Commande | Description |
|----------|-------------|
| `tests [fichier]` | Lance pytest |
| `tests-cov` | Tests avec couverture de code |
| `shell` | Shell dans le conteneur Airflow |
| `python` | Console Python interactive |

---

## Variables Airflow

### AMUE

| Variable | Défaut | Rôle |
|----------|--------|------|
| `amue_import_schedule` | `0 2 * * *` | Cron import principal |
| `amue_import_batch_size` | `5000` | Lignes par appel API |
| `amue_import_parallel_workers` | `1` | Tables en parallèle |
| `amue_polling_interval_minutes` | `10` | Fréquence sensor (minutes) |
| `amue_max_wait_hours` | `6` | Timeout sensor (heures) |
| `amue_force_import` | `false` | Bypass sensor (dev uniquement) |
| `amue_api_max_retries` | `3` | Tentatives sur erreur API |
| `amue_api_retry_delay_seconds` | `30` | Délai entre retries |
| `amue_pre_import_dags` | `[]` | DAGs déclenchés avant import |
| `amue_post_import_dags` | `[]` | DAGs déclenchés après import |
| `amue_report_recipients` | — | Destinataires email |

### ECC

| Variable | Défaut | Rôle |
|----------|--------|------|
| `ecc_import_schedule` | `0 4 * * *` | Cron import ECC |
| `ecc_import_batch_size` | `5000` | Lignes par batch Oracle |
| `ecc_report_recipients` | — | Destinataires email ECC |

### Autres

| Variable | Défaut | Rôle |
|----------|--------|------|
| `universite` | — | Code université pour les endpoints API |
| `smtp_host` | `mailhog` | Serveur SMTP |
| `smtp_port` | `1025` | Port SMTP |
| `amue_sync_schedule` | `0 6 * * *` | Cron synchronisation Blue/Green |
| `amue_monitor_schedule` | `0 22 * * *` | Cron monitoring API |

Liste complète : `config/airflow_variables.json`.

---

## Interfaces web

| Interface | URL | Identifiants |
|-----------|-----|--------------|
| Airflow UI | http://localhost:8080 | airflow / airflow (dev) |
| MailHog | http://localhost:8025 | aucun |
| Kafka UI | http://localhost:8090 | aucun |

---

## Points clés de conception

| Principe | Description |
|----------|-------------|
| DAGs = orchestration pure | Toute la logique métier est dans `plugins/` |
| Pas de suppression | UPSERT uniquement (`INSERT ON CONFLICT UPDATE`) |
| Switch atomique | DROP + CREATE en transaction unique pour les vues |
| Verrou PostgreSQL | `UPDATE ... RETURNING id` atomique, pas de race condition |
| Double fingerprint | `fingerprint_api` + `fingerprint_local` pour détecter les changements de structure |
| Import différentiel | DELTA si colonne delta configurée + `last_report_start` disponible |
| Protection sifac_plus | Lignes `_source = 'sifac_plus'` jamais écrasées par ECC |
| Traçabilité | Colonnes `_source` et `_imported_at` sur toutes les tables |
