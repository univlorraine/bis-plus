# Guide de mise à jour — DemoDAGS

Ce document décrit comment mettre à jour le projet vers une nouvelle version :
procédure automatisée, procédure manuelle, migrations SQL applicatives,
et rollback en cas de problème.

## Table des matières

1. [Avant de commencer](#1-avant-de-commencer)
2. [Procédure automatisée (recommandée)](#2-procédure-automatisée-recommandée)
3. [Procédure manuelle](#3-procédure-manuelle)
4. [Migrations SQL applicatives](#4-migrations-sql-applicatives)
5. [Rollback](#rollback)
6. [Vérification post-mise-à-jour](#vérification-post-mise-à-jour)

---

## 1. Avant de commencer

**Sauvegarder** avant toute mise à jour :

```bash
./manage.sh db-backup          # dump horodaté de la base métier
./manage.sh config-backup      # variables + connexions + .env
```

**Vérifier** que le système est dans un état stable :

```bash
./manage.sh health
```

Tous les services doivent être `healthy`. Si un import est en cours
(`import_in_progress = true` dans `splus_admin.amue_state`), attendre
qu'il se termine.

**Identifier la release cible** :

```bash
# Voir les releases disponibles
git tag -l --sort=-version:refname | head -10

# Ou sur GitHub : releases publiées avec leurs notes de mise à jour
```

---

## 2. Procédure automatisée (recommandée)

```bash
./manage.sh update [tag]
```

Sans `[tag]`, met à jour vers la dernière release publiée.

Cette commande réalise dans l'ordre :

| Étape | Action |
|-------|--------|
| 1 | `git fetch` + checkout du tag cible |
| 2 | `docker compose build` (rebuild de l'image Airflow si `requirements.txt` a changé) |
| 3 | `./manage.sh db-migrate` (applique les migrations SQL en attente) |
| 4 | `docker compose up -d --wait` (redémarre tous les services) |
| 5 | `airflow variables import config/airflow_variables.json` (ajoute les nouvelles variables sans écraser les existantes) |
| 6 | Rapport de mise à jour |

> La commande est idempotente : la rejouer après une erreur reprend
> là où elle s'est arrêtée.

---

## 3. Procédure manuelle

À utiliser si `./manage.sh update` n'est pas disponible ou pour auditer
chaque étape individuellement.

### Étape 1 — Récupérer le code

```bash
git fetch --tags
git checkout <tag>        # ex : git checkout v2.1.0
```

### Étape 2 — Rebuilder l'image Docker (si besoin)

```bash
# Vérifier si requirements.txt a changé depuis la version précédente
git diff <ancienne-version>..<nouvelle-version> -- requirements.txt

# Si oui (ou par précaution) :
docker compose build --no-cache airflow-apiserver
docker compose build --no-cache airflow-scheduler
```

### Étape 3 — Appliquer les migrations SQL

```bash
./manage.sh db-migrate
```

Voir section [4. Migrations SQL applicatives](#4-migrations-sql-applicatives)
pour le détail du mécanisme.

### Étape 4 — Redémarrer les services

```bash
docker compose up -d --wait
```

### Étape 5 — Importer les nouvelles variables Airflow

```bash
docker compose exec airflow-apiserver airflow variables import \
  /opt/airflow/config/airflow_variables.json
```

> L'import est non-destructif : les variables existantes avec des valeurs
> personnalisées ne sont **pas** écrasées.

### Étape 6 — Vérifier

Voir section [6. Vérification post-mise-à-jour](#6-vérification-post-mise-à-jour).

---

## 4. Migrations SQL applicatives

Les évolutions du schéma applicatif (`splus_admin`, `splus_blue`/`splus_green`, etc.)
sont gérées via des fichiers SQL dans `scripts/sql/migrations/`.

### Convention

```
NNNN_description_courte.sql
```

- `NNNN` : séquence à 4 chiffres, strictement croissante (`0001`, `0002`, …)
- Chaque fichier est **idempotent** (peut être rejoué sans erreur)

### Appliquer manuellement

```bash
# Voir quelles migrations sont en attente
./manage.sh db-migrate --dry-run

# Appliquer
./manage.sh db-migrate
```

Ou directement via psql :

```bash
psql -h localhost -p 5433 -U datauser -d business_data \
  -f scripts/sql/migrations/0001_description.sql
```

### Écrire une nouvelle migration

Voir `scripts/sql/migrations/README.md` pour les patterns idempotents
à utiliser (`ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`,
bloc `DO $$ BEGIN ... END $$` pour les opérations sans variante `IF NOT EXISTS`).

**Règle fondamentale** : aucune `DELETE`, aucune `TRUNCATE`. Le projet
ne supprime jamais de données (UPSERT only).

---

## Rollback

Deux cas distincts :

### 5.1 Rollback de données (import foireux)

Si un import vient de produire des données incorrectes, restaurer le
schéma précédent sans toucher au code :

```bash
./manage.sh trigger amue_rollback
```

Ou via l'UI : DAG `amue_rollback` → ▶

**Condition** : disponible uniquement jusqu'au prochain import réussi.
Au-delà, restaurer depuis une sauvegarde (`./manage.sh db-restore`).

**Vérification** :

```sql
SELECT active_schema, last_switch_timestamp
  FROM splus_admin.amue_state WHERE id = 1;
```

### 5.2 Rollback de release (mauvaise mise à jour)

Si la mise à jour elle-même pose problème (bug de code, migration SQL
défaillante, incompatibilité de dépendances) :

```bash
# 1. Revenir au code de la version précédente
git checkout <ancienne-version>

# 2. Rebuilder si besoin
docker compose build

# 3. Restaurer la base si une migration a été appliquée par erreur
#    (uniquement si la migration est destructive — rarissime dans ce projet)
./manage.sh db-restore <fichier-dump-de-sauvegarde>

# 4. Redémarrer
docker compose up -d --wait

# 5. Vérifier
./manage.sh health
```

> Les migrations de ce projet sont conçues pour être non-destructives
> (jamais de `DROP COLUMN`, jamais de `TRUNCATE`). Un rollback de release
> ne nécessite généralement **pas** de restauration de base.

---

## 6. Vérification post-mise-à-jour

```bash
# Services
./manage.sh health

# Pas d'erreur de parsing de DAG
docker compose exec airflow-apiserver airflow dags list-import-errors

# Tous les DAGs attendus sont présents
docker compose exec airflow-apiserver airflow dags list | grep amue

# Migrations appliquées
psql -h localhost -p 5433 -U datauser -d business_data \
  -c "SELECT version, applied_at FROM splus_admin.schema_migrations ORDER BY version;"

# État Blue/Green intact
psql -h localhost -p 5433 -U datauser -d business_data \
  -c "SELECT active_schema, import_in_progress, last_successful_run FROM splus_admin.amue_state WHERE id = 1;"

# Tables configurées
psql -h localhost -p 5433 -U datauser -d business_data \
  -c "SELECT count(*) FROM splus_admin.amue_tables WHERE enabled = true AND setup_status = 'ready';"
```

En cas de doute, lancer le diagnostic complet :

```bash
./manage.sh diagnose
```

Et déclencher un import de test :

```bash
./manage.sh var-set amue_force_import true
./manage.sh trigger amue_multi_table_import
# ... puis impérativement :
./manage.sh var-set amue_force_import false
```
