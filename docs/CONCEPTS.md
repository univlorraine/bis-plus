# Concepts DemoDAGS

Référence centralisée des concepts partagés par `DEPLOYMENT.md`,
`OPERATIONS.md`, `TROUBLESHOOTING.md` et `technique.md`. Les sections détaillées
ci-dessous doivent être **la seule source de vérité** ; les autres docs
renvoient vers ce fichier pour éviter toute divergence.

---

## Statuts de table

Chaque ligne de `splus_admin.amue_tables` porte un `setup_status` qui pilote
le comportement des DAGs :

| Statut    | Signification                                                    | Conséquence                                                |
|-----------|------------------------------------------------------------------|------------------------------------------------------------|
| `pending` | Jamais initialisée (valeur par défaut à la création).            | `amue_table_setup` doit traiter la table avant l'import.   |
| `ready`   | Initialisée avec succès, fingerprints à jour, prête pour import. | `amue_multi_table_import` peut l'importer.                 |
| `blocked` | Changement de structure détecté (fingerprint mismatch).          | Intervention manuelle requise avant de débloquer l'import. |

Voir [TROUBLESHOOTING.md](TROUBLESHOOTING.md) pour les procédures de déblocage (forçage en
`pending`, désactivation temporaire, etc.).

---

## Schémas blue/green

Le projet maintient **deux schémas de tables** (`splus_blue` et `splus_green`) et
un schéma de vues (`splus`) qui pointe atomiquement vers l'un ou l'autre.

- L'état (schéma actif, verrou d'import, timestamp de dernier import) est persisté
  dans `splus_admin.amue_state` — **pas** dans des variables Airflow.
- Le basculement de vues est atomique (DROP + CREATE dans une même transaction).
- Un rollback reste possible jusqu'au prochain import (DAG `amue_rollback`).

---

## Tables d'administration (`splus_admin`)

Deux tables pilotent l'ensemble du système :

### `splus_admin.amue_state` (singleton, `id = 1`)

État global de la stack blue/green :

| Colonne                  | Description                                                       |
|--------------------------|-------------------------------------------------------------------|
| `active_schema`          | `blue` ou `green` — schéma actuellement exposé par les vues       |
| `import_in_progress`     | Verrou d'import (empêche deux imports simultanés)                 |
| `import_started_at`      | Timestamp de démarrage de l'import en cours                       |
| `last_report_start`      | Référence delta (timestamp `start` de l'API) — fenêtre de l'import différentiel |
| `last_finish_timestamp`  | Timestamp du dernier rapport AMUE traité                          |
| `last_successful_run`    | Horodatage du dernier import entièrement réussi                   |
| `last_switch_timestamp`  | Dernier basculement blue/green                                    |
| `last_sync_timestamp`    | Dernière synchronisation blue ↔ green                             |
| `import_correlation_id`  | ID de corrélation pour le tracing des logs (= `run_id` Airflow)   |
| `updated_at`             | Horodatage de la dernière mise à jour de la ligne                 |

### `splus_admin.amue_tables`

Liste des tables à importer et leur état de configuration :

| Colonne              | Description                                                           |
|----------------------|-----------------------------------------------------------------------|
| `table_name`         | Nom de la table (clé primaire)                                        |
| `enabled`            | `true` = incluse dans l'import ; `false` = ignorée                    |
| `primary_key`        | Colonnes de clé primaire pour l'UPSERT (prioritaires sur l'API)       |
| `delta`              | Colonne de date pour import différentiel ; vide = import complet      |
| `setup_status`       | `pending` / `ready` / `blocked` — voir [Statuts de table](#statuts-de-table) |
| `fingerprint_api`    | Empreinte de structure côté API (calculée par `amue_table_setup`)     |
| `fingerprint_local`  | Empreinte de structure côté PostgreSQL (calculée par `amue_table_setup`) |
| `ecc_query`          | `NULL` = table AMUE ; non-`NULL` = requête SQL Oracle (table ECC)     |

> Toute modification de ces tables (ajout, activation, clé primaire) doit être
> suivie d'un déclenchement de `amue_table_setup` pour recalculer les fingerprints.

---

## Vérifier PostgreSQL

Séquence standard pour valider une installation ou diagnostiquer un incident.

> Les variables `$PGHOST`, `$PGUSER`, `$PGDATABASE` correspondent aux valeurs du `.env`
> (`POSTGRES_DATA_HOST`, `POSTGRES_DATA_USER`, `POSTGRES_DATA_DB`). En Docker Compose,
> utiliser directement `./manage.sh db-shell` qui les injecte automatiquement.

```bash
# 1. Service actif (Docker)
docker compose ps postgres-data

# 2. Connexion rapide via manage.sh
./manage.sh db-shell

# 3. Schémas en place (dans le shell postgres)
\dn
# Attendu : splus, splus_admin, splus_blue, splus_green

# 4. Table d'état blue/green
SELECT active_schema, import_in_progress, last_successful_run
  FROM splus_admin.amue_state WHERE id = 1;

# 5. Nombre de connexions actives (utile si "too many clients")
SELECT count(*), state FROM pg_stat_activity GROUP BY state;
```

---

## Vérifier la connexion Airflow → PostgreSQL

```bash
airflow connections get postgres_data
# Champ 'host', 'schema', 'login' doivent correspondre à l'environnement cible
```

Le `schema` dans la connexion Airflow est ignoré : chaque `PostgresHook` est
créé avec `options='-c search_path=<schema>'` via `create_postgres_hook()` /
`resolve_postgres_hook()` (voir `plugins/common/infrastructure/database/hooks.py`).
