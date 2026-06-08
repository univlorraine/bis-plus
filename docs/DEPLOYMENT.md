# Guide de Déploiement DemoDAGS

Ce document décrit les étapes nécessaires pour déployer le système DemoDAGS d'import AMUE.

## Table des matières

1. [Prérequis](#prérequis)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Vérification](#vérification)
5. [Mise à jour](#mise-à-jour)

---

## Prérequis

### Logiciels requis

| Composant | Version minimale | Recommandée |
|-----------|-----------------|-------------|
| Python | 3.9 | 3.10+ |
| PostgreSQL | 13 | 15+ |
| Apache Airflow | 3.x | 3.x |
| Docker (optionnel) | 20.10 | 24+ |

### Ressources système

- **CPU** : 2 cores minimum (4 recommandés)
- **RAM** : 4 GB minimum (8 GB recommandés)
- **Disque** : 20 GB minimum pour les données

### Accès réseau

- Accès à l'API AMUE (endpoint configuré dans les variables)
- Port 5432 pour PostgreSQL
- Port 8080 pour l'interface Airflow

---

## Installation

### Étape 1 : Cloner le dépôt

```bash
git clone <repository-url> DemoDAGS
cd DemoDAGS
```

### Étape 2 : Créer l'environnement Python

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# ou
.\venv\Scripts\activate   # Windows

pip install -r requirements.txt
```

### Étape 3 : Configurer PostgreSQL

1. Créer la base de données et les utilisateurs :

```sql
-- Base de données
CREATE DATABASE business_data;

-- Utilisateur Airflow (métadonnées Airflow)
CREATE USER airflow WITH PASSWORD 'votre_mot_de_passe';
GRANT ALL PRIVILEGES ON DATABASE business_data TO airflow;

-- Utilisateur applicatif (lecture/écriture des données AMUE)
CREATE USER datauser WITH PASSWORD 'votre_mot_de_passe_datauser';
GRANT CONNECT ON DATABASE business_data TO datauser;
```

2. Exécuter le script d'initialisation :

```bash
psql -U airflow -d business_data -f scripts/sql/init_db.sql
```

Ce script crée :
- Le schéma `splus_admin` avec les tables `amue_state` et `amue_tables`
- Les schémas `splus_blue` et `splus_green` (tables de données)
- Le schéma `splus` (vues publiques pointant vers blue ou green)
- Les permissions pour `datauser` sur tous les schémas

3. Activer les tables à importer (décommenter dans `init_db.sql`) :

```sql
-- Dans le fichier init_db.sql, décommenter le bloc INSERT commenté
-- puis réexécuter, ou insérer directement :
INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta) VALUES
    ('CSKS',  true, 'KOKRS,KOSTL,DATBI', ''),
    ('BKPF',  true, 'BUKRS,BELNR,GJAHR', 'cpudt'),
    ('LFA1',  true, 'LIFNR',             '')
ON CONFLICT (table_name) DO NOTHING;
```

> Le fichier `init_db.sql` contient en commentaire la liste complète des 35+ tables configurées pour cet environnement.


### Étape 4 : Configurer Airflow

1. Initialiser la base Airflow :

```bash
airflow db migrate
```

2. Créer un utilisateur admin :

```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

3. Configurer les connexions dans Airflow UI ou via CLI :

```bash
# Connexion PostgreSQL (conn_id: postgres_data)
airflow connections add 'postgres_data' \
    --conn-type 'postgres' \
    --conn-host 'postgres-data' \
    --conn-schema 'business_data' \
    --conn-login 'datauser' \
    --conn-password 'votre_mot_de_passe_datauser' \
    --conn-port '5433' \
    --conn-extra '{"options": "-c search_path=splus"}'

# Connexion API AMUE (conn_id: oauth_api)
airflow connections add 'oauth_api' \
    --conn-type 'http' \
    --conn-host 'https://sandbox.api.amue.fr' \
    --conn-extra '{"token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token", "api_base_url": "https://sandbox.api.amue.fr", "client_id": "...", "client_secret": "..."}'

# Connexion Oracle ECC (optionnel, conn_id: oracle_data)
airflow connections add 'oracle_data' \
    --conn-type 'odbc' \
    --conn-host 'base-oracle.domaine.fr' \
    --conn-schema 'Txx' \
    --conn-port '1521'
```

> Ces connexions peuvent aussi être importées depuis `config/airflow_connections.json` via l'UI Airflow (Admin → Connections → Import).


### Étape 5 : Configurer les variables Airflow

Importer les variables depuis le fichier de configuration :

```bash
airflow variables import config/airflow_variables.json
```

Variables importantes à personnaliser :
- `universite` : Code de l'établissement
- `environment` : `dev` ou `production`


---

## Configuration

### Variables Airflow principales

| Variable | Description | Défaut | Exemple |
|----------|-------------|--------|---------|
| `universite` | Code établissement | — | `univ` |
| `environment` | Environnement | — | `production` |
| `api_endpoint_admin` | URL API admin (polling) | — | `finances/cdv/v1/preprod/${univ}/admin` |
| `api_endpoint_table` | URL API par table | — | `finances/cdv/v1/preprod/${univ}/table` |
| `amue_import_schedule` | Cron de l'import principal | `0 2 * * *` | `0 2 * * *` |
| `amue_sync_schedule` | Cron de la synchro B/G | `0 6 * * *` | `0 6 * * *` |
| `amue_monitor_schedule` | Cron du monitoring API | `0 22 * * *` | `0 22 * * *` |
| `ecc_import_schedule` | Cron de l'import ECC | `0 4 * * *` | `0 4 * * *` |
| `amue_report_recipients` | Destinataires des emails | — | `admin@univ.fr` |
| `ecc_report_recipients` | Destinataires emails ECC | — | `admin@univ.fr` |
| `amue_polling_interval_minutes` | Intervalle du sensor (min) | `10` | `5` |
| `amue_max_wait_hours` | Timeout du sensor (h) | `6` | `12` |
| `amue_polling_exponential_backoff` | Backoff exponentiel sensor | `false` | `true` |
| `amue_polling_max_backoff_minutes` | Backoff max (min) | `60` | `60` |
| `amue_import_batch_size` | Lignes par batch API | `5000` | `10000` |
| `amue_import_parallel_workers` | Tables en parallèle | `1` | `5` |
| `amue_api_max_retries` | Tentatives retry API | `3` | `5` |
| `amue_api_retry_delay_seconds` | Délai entre retries (s) | `30` | `60` |
| `amue_force_import` | Forcer import (ignore sensor) | `false` | `true` |
| `amue_pre_import_dags` | DAGs à déclencher avant import | `[]` | `["amue_table_setup"]` |
| `amue_post_import_dags` | DAGs à déclencher après import | `[]` | `["amue_refresh_views"]` |
| `smtp_host` | Serveur SMTP | `mailhog` | `smtp.univ.fr` |
| `smtp_port` | Port SMTP | `1025` | `587` |
| `smtp_mail_from` | Expéditeur emails | `airflow@amue.local` | `amue@univ.fr` |
| `smtp_use_tls` | TLS SMTP | `false` | `true` |

### Configuration des tables

La liste des tables à importer est gérée dans la table PostgreSQL `splus_admin.amue_tables` (pas dans une variable Airflow). Elle est initialisée par `scripts/sql/init_db.sql` et peut être modifiée via SQL :

```sql
-- Activer / désactiver une table
UPDATE splus_admin.amue_tables SET enabled = true WHERE table_name = 'CSKS';

-- Modifier les clés primaires
UPDATE splus_admin.amue_tables SET primary_key = 'bukrs,kostl' WHERE table_name = 'CSKS';
```

### Architecture Blue/Green

L'architecture blue/green est toujours active. Aucune configuration supplémentaire n'est nécessaire.

### Chaînage de DAGs (pré/post import)

Pour déclencher des DAGs automatiquement avant ou après chaque import :

```bash
# DAG(s) à exécuter séquentiellement AVANT l'import
airflow variables set amue_pre_import_dags '["amue_table_setup"]'

# DAG(s) à exécuter séquentiellement APRÈS un import réussi
airflow variables set amue_post_import_dags '["amue_refresh_views"]'
```

Laisser `[]` (valeur par défaut) pour désactiver le chaînage.

---

## Vérification

### Vérifier l'installation Python

```bash
python -c "import amue; print('OK')"
```

### Vérifier la connexion PostgreSQL

```bash
airflow connections test postgres_data
```

### Vérifier le DAG

```bash
airflow dags list | grep amue
airflow dags list-import-errors
```


### Exécuter les tests

```bash
pytest tests/ -v
```

### Vérifier les schémas Blue/Green

```sql
SELECT schema_name FROM information_schema.schemata
WHERE schema_name IN ('splus', 'splus_blue', 'splus_green');
```


---

## Mise à jour

La mise à jour du projet (code, dépendances, image Docker, schéma SQL applicatif,
métadonnées Airflow, variables) est entièrement décrite dans
**[UPGRADE.md](UPGRADE.md)**, qui couvre :

- la procédure automatisée `./manage.sh update [tag]` (cible toujours une release GitHub
  publiée — jamais une branche ou un commit)
- la procédure manuelle équivalente, étape par étape, si besoin d'auditer ou de dépanner
- comment écrire une migration SQL (`scripts/sql/migrations/`)
- la marche à suivre en cas de rollback : DAG `amue_rollback` (problème de données après
  un import) versus rollback complet du projet (mauvaise release — code/schéma/dépendances)


---

## Troubleshooting

Voir [TROUBLESHOOTING.md](TROUBLESHOOTING.md) pour les problèmes courants.

## Support

Pour toute question :
- Consulter la documentation dans `docs/`
- Vérifier les logs Airflow
- Contacter l'équipe de support
