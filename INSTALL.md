# Guide d'installation — DemoDAGS

Pipeline Airflow d'import de données AMUE/ECC vers PostgreSQL avec architecture Blue/Green.

---

## Prérequis

| Outil | Version minimale | Vérification |
|-------|-----------------|--------------|
| Docker | 24+ (avec plugin Compose) | `docker compose version` |
| jq | 1.6+ | `jq --version` |
| psql | optionnel | `psql --version` |

**Ressources système :**
- 4 Go RAM libres minimum
- Ports disponibles : `8080` (Airflow UI), `5432` (PostgreSQL Airflow), `5433` (PostgreSQL données), `8025` (MailHog UI), `1025` (SMTP)

**Système d'exploitation :** Linux, macOS, ou WSL2 (Windows natif non supporté)

---

## Méthode 1 — Installation via scripts (recommandée)

### Étape 1 — Cloner le projet

```bash
git clone <repo-url> DemoDAGS
cd DemoDAGS
```

### Étape 2 — Lancer le setup interactif

```bash
chmod +x manage.sh
./manage.sh setup
```

Le script `quick_setup.sh` pose une série de questions et prend en charge l'intégralité de la configuration :

| Question | Exemple de réponse |
|----------|--------------------|
| Environnement | `1` (dev/sandbox) ou `2` (production) |
| Code université | `univ` |
| Destinataires rapports | `admin@univ.fr` |
| Client ID OAuth AMUE | `<votre_client_id>` |
| Client Secret OAuth AMUE | `<votre_client_secret>` |
| Mot de passe PostgreSQL données | `<datapass>` |
| Configurer Oracle ECC ? | `o` / `n` |

**Ce que le script réalise automatiquement :**

1. Génère une clé Fernet et crée le fichier `.env`
2. Démarre tous les services Docker (`docker compose up -d --wait`)
3. Exécute `scripts/sql/init_db.sql` sur `postgres-data`
4. Importe les variables Airflow depuis `config/airflow_variables.json`
5. Crée les connexions Airflow (structure uniquement)
6. Injecte les credentials chiffrés directement dans la base Airflow (jamais sur disque)
7. Insère la liste des tables dans `splus_admin.amue_tables`

### Étape 3 — Vérifier l'installation

```bash
./manage.sh health
```

Accès aux interfaces :
- Airflow UI : http://localhost:8080 (airflow / airflow par défaut)
- MailHog UI : http://localhost:8025



---

## Méthode 2 — Installation 100% manuelle

### Étape 1 — Cloner le projet

```bash
git clone <repo-url> DemoDAGS
cd DemoDAGS
```

### Étape 2 — Créer et configurer `.env`

```bash
cp .env.example .env
```

Générer une clé Fernet :

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Éditer `.env` et renseigner :

```bash
# UID de votre utilisateur Unix (évite les problèmes de permissions sur les volumes)
AIRFLOW_UID=1001                        # résultat de : id -u

# Clé Fernet (chiffrement des credentials en base Airflow) — générée à l'étape précédente
AIRFLOW__CORE__FERNET_KEY=<clé_fernet>

# Credentials interface web Airflow (CHANGER en production)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# SMTP — mailhog en dev (emails capturés localement), serveur réel en prod
SMTP_HOST=mailhog
SMTP_PORT=1025

# Credentials OAuth AMUE (injectés chiffrés dans Airflow, jamais en clair sur disque)
AMUE_CLIENT_ID=<votre_client_id>
AMUE_CLIENT_SECRET=<votre_client_secret>

# Mot de passe de l'utilisateur datauser PostgreSQL (base métier)
PG_DATA_PASSWORD=datapass
```

Variables PostgreSQL (avec valeurs par défaut dans `docker-compose.yml`) :

| Variable | Défaut | Rôle |
|----------|--------|------|
| `POSTGRES_USER` | `airflow` | Utilisateur base de métadonnées Airflow |
| `POSTGRES_PASSWORD` | `airflow` | Mot de passe base Airflow |
| `POSTGRES_DB` | `airflow` | Nom de la base Airflow |
| `PG_DATA_USER` | `datauser` | Utilisateur base métier |
| `PG_DATA_PASSWORD` | `datapass` | Mot de passe base métier |
| `PG_DATA_DB` | `business_data` | Nom de la base métier |

> Ces variables n'ont pas besoin d'être définies dans `.env` sauf pour surcharger les défauts.

### Étape 3 — Démarrer les services Docker

```bash
# Initialisation Airflow (unique, crée la base de métadonnées)
docker compose up airflow-init

# Démarrer tous les services en arrière-plan
docker compose up -d
```

Attendre que tous les services soient `healthy` :

```bash
docker compose ps
```

Services attendus : `postgres`, `postgres-data`, `airflow-apiserver`, `airflow-scheduler`,
`airflow-dag-processor`, `airflow-triggerer`, `mailhog`.


> Si un service reste en `starting` plus de 2 minutes, consulter ses logs :
> ```bash
> docker compose logs <nom-du-service> --tail 50
> ```

### Étape 4 — Initialiser la base de données métier

Ce script crée les schémas Blue/Green et les tables d'administration.

**Via le conteneur :**

```bash
docker compose exec postgres-data psql -U datauser -d business_data \
  -f /scripts/sql/init_db.sql
```

**Via psql local :**

```bash
psql -h localhost -p 5433 -U datauser -d business_data \
  -f scripts/sql/init_db.sql
```

**Ce que le script crée :**

| Objet | Rôle |
|-------|------|
| Schéma `splus` | Interface publique (vues uniquement) |
| Schéma `splus_blue` | Tables Blue (import atomique) |
| Schéma `splus_green` | Tables Green (import atomique) |
| Schéma `splus_admin` | Administration et état |
| Table `splus_admin.amue_state` | État centralisé Blue/Green |
| Table `splus_admin.amue_tables` | Configuration des tables à importer |

### Étape 5 — Configurer les variables Airflow

#### Option A — Via l'UI Airflow


Aller sur http://localhost:8080 → **Admin** → **Variables** → bouton **+**

Créer chaque variable (champs `Key` et `Val`) :

| Key | Valeur |
|-----|--------|
| `universite` | `univ` |
| `amue_api_source` | `entrepot` |
| `api_endpoint_entrepot` | `finances/entrepotdedonnees/v1/preprod/${univ}` |
| `api_endpoint_admin` | `finances/cdv/v1/preprod/${univ}/admin` *(source `cdv` uniquement)* |
| `api_endpoint_table` | `finances/cdv/v1/preprod/${univ}/table` *(source `cdv` uniquement)* |
| `amue_import_schedule` | `0 2 * * *` |
| `amue_sync_schedule` | `0 6 * * *` |
| `amue_monitor_schedule` | `0 22 * * *` |
| `amue_import_batch_size` | `5000` |
| `amue_import_parallel_workers` | `1` |
| `amue_api_max_retries` | `3` |
| `amue_api_retry_delay_seconds` | `30` |
| `amue_polling_interval_minutes` | `10` |
| `amue_max_wait_hours` | `6` |
| `amue_polling_exponential_backoff` | `false` |
| `amue_polling_max_backoff_minutes` | `60` |
| `amue_force_import` | `false` |
| `smtp_host` | `mailhog` |
| `smtp_port` | `1025` |
| `smtp_mail_from` | `airflow@amue.local` |
| `smtp_use_tls` | `false` |
| `smtp_timeout` | `30` |
| `amue_report_recipients` | `admin@example.com` |
| `ecc_report_recipients` | `admin@example.com` |
| `amue_reports_dir` | `/opt/airflow/logs/reports` |
| `ecc_import_batch_size` | `5000` |
| `ecc_import_schedule` | `0 4 * * *` |
| `TYPE_MAPPING_SQLITE_TO_POSTGRES` | *(copier le JSON depuis `config/airflow_variables.json`)* |

> Pour les objets JSON (comme `TYPE_MAPPING_SQLITE_TO_POSTGRES`), coller le JSON directement dans le champ `Val`.

#### Option B — Via CLI (import en masse)

```bash
docker compose exec airflow-apiserver airflow variables import \
  /opt/airflow/config/airflow_variables.json
```

#### Option C — Via API REST

```bash
curl -s -X POST http://localhost:8080/api/v1/variables \
  -u airflow:airflow \
  -H "Content-Type: application/json" \
  -d '{"key": "universite", "value": "univ"}'
```

Répéter pour chaque variable du tableau ci-dessus.

### Étape 6 — Créer les connexions Airflow

#### Option A — Via l'UI Airflow


Aller sur http://localhost:8080 → **Admin** → **Connections** → bouton **+**

---

**Connexion `oauth_api`** — API AMUE (OAuth2 client_credentials)

| Champ           | Valeur                                                                                                              |
|-----------------|---------------------------------------------------------------------------------------------------------------------|
| Connection Id   | `oauth_api`                                                                                                         |
| Connection Type | `HTTP`                                                                                                              |
| Host            | `https://sandbox.api.amue.fr`                                                                                       |
| Login           | `<client_id>`                                                                                                       |
| Password        | `<client_secret>`                                                                                                   |
| Extra           | `{"token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token", "api_base_url": "https://sandbox.api.amue.fr"}` |

> Le champ **Extra** est obligatoire — il contient l'URL du serveur OAuth et l'URL de base de l'API.

---

**Connexion `postgres_data`** — Base de données métier

| Champ           | Valeur                                 |
|-----------------|----------------------------------------|
| Connection Id   | `postgres_data`                        |
| Connection Type | `Postgres`                             |
| Host            | `postgres-data`                        |
| Port            | `5433`                                 |
| Schema          | `business_data`                        |
| Login           | `datauser`                             |
| Password        | `datapass` *(ou valeur personnalisée)* |

---

**Connexion `oracle_data`** — Oracle ECC *(optionnel)*

| Champ           | Valeur                   |
|-----------------|--------------------------|
| Connection Id   | `oracle_data`            |
| Connection Type | `ODBC`                   |
| Host            | `base-oracle.domaine.fr` |
| Port            | `1521`                   |
| Schema          | `Txx`                    |
| Login           | `<user>`                 |
| Password        | `<password>`             |

---

#### Option B — Via CLI dans le conteneur

```bash
# Connexion API AMUE
docker compose exec airflow-apiserver airflow connections add oauth_api \
  --conn-type http \
  --conn-host https://sandbox.api.amue.fr \
  --conn-login <client_id> \
  --conn-password <client_secret> \
  --conn-extra '{"token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token", "api_base_url": "https://sandbox.api.amue.fr"}'

# Connexion PostgreSQL données
docker compose exec airflow-apiserver airflow connections add postgres_data \
  --conn-type postgres \
  --conn-host postgres-data \
  --conn-port 5433 \
  --conn-schema business_data \
  --conn-login datauser \
  --conn-password datapass \
  --conn-extra '{"options": "-c search_path=splus"}'

# Connexion Oracle ECC (optionnel)
docker compose exec airflow-apiserver airflow connections add oracle_data \
  --conn-type odbc \
  --conn-host base-oracle.domaine.fr \
  --conn-port 1521 \
  --conn-schema Txx \
  --conn-login <user> \
  --conn-password <password>
```

### Étape 7 — Insérer les tables à importer

Les tables à importer sont configurées dans `splus_admin.amue_tables`.
La liste complète est disponible en commentaire dans `scripts/sql/init_db.sql` (lignes 108–146).

**Exemple :**

```bash
psql -h localhost -p 5433 -U datauser -d business_data << 'EOF'
INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta) VALUES
  ('BKPF',      true,  'BUKRS,BELNR,GJAHR',                                                'cpudt'),
  ('CEPC',      true,  'PRCTR,DATBI,KOKRS',                                                ''),
  ('CSKS',      true,  'KOKRS,KOSTL,DATBI',                                                ''),
  ('FMFCTR',   true,  'FIKRS,FICTR,DATBIS',                                               ''),
  ('FMIFIIT',  true,  'FIKRS,BTART,RLDNR,GJAHR,STUNR',                                   ''),
  ('KNA1',      true,  'KUNNR',                                                             ''),
  ('LFA1',      true,  'LIFNR',                                                             ''),
  ('PA0001',   true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                       ''),
  ('USR02',    true,  'BNAME',                                                             '')
ON CONFLICT (table_name) DO NOTHING;
EOF
```

> La colonne `delta` contient le nom du champ de date utilisé pour l'import différentiel.
> Laisser vide (`''`) pour un import complet (FULL) à chaque exécution.

**Via l'UI Airflow** : le DAG `amue_table_setup` peut également créer les tables manquantes
et vérifier les empreintes (fingerprints) à la première exécution.

### Étape 8 — Vérification

```bash
# Tous les services healthy
docker compose ps

# Santé Airflow
curl -s -u airflow:airflow http://localhost:8080/api/v1/health | jq .

# Variables présentes
docker compose exec airflow-apiserver airflow variables list

# Connexions créées
docker compose exec airflow-apiserver airflow connections list

# État Blue/Green initialisé
psql -h localhost -p 5433 -U datauser -d business_data \
  -c "SELECT id, active_schema, import_in_progress, updated_at FROM splus_admin.amue_state;"

# Tables configurées
psql -h localhost -p 5433 -U datauser -d business_data \
  -c "SELECT table_name, enabled, setup_status FROM splus_admin.amue_tables ORDER BY table_name;"
```

Accès aux interfaces :
- **Airflow UI** : http://localhost:8080 (login: `airflow` / `airflow`)
- **MailHog** : http://localhost:8025 (capture des emails de rapport)


### Étape 9 — Premier import


Avant le premier import de données, initialiser les tables (fingerprints + création des tables PostgreSQL) :

```bash
# Initialisation des tables (obligatoire avant le 1er import)
docker compose exec airflow-apiserver airflow dags trigger amue_table_setup
```

Attendre que ce DAG se termine (statut `success` dans l'UI), puis déclencher l'import :

```bash
# Import principal
docker compose exec airflow-apiserver airflow dags trigger amue_multi_table_import
```

**Via l'UI :** http://localhost:8080 → DAG `amue_table_setup` → ▶, puis DAG `amue_multi_table_import` → ▶

> Pour tester sans attendre que l'API publie un nouveau rapport, activer temporairement le bypass du sensor :
> ```bash
> docker compose exec airflow-apiserver airflow variables set amue_force_import true
> # Remettre à false après le test
> docker compose exec airflow-apiserver airflow variables set amue_force_import false
> ```

---

## Commandes utiles post-installation

```bash
# Arrêter tous les services
./manage.sh stop

# Redémarrer
./manage.sh restart

# Consulter les logs Airflow
./manage.sh logs

# Diagnostic système
./manage.sh diagnose

# Mettre à jour une connexion (ex: renouveler un secret OAuth)
./manage.sh conn-update

# Déclencher un rollback Blue/Green
./manage.sh trigger amue_rollback

# Lancer les tests
./manage.sh test

# Accès shell PostgreSQL données
./manage.sh db-shell
```

---

---

## Dépannage post-installation

### Service unhealthy ou qui ne démarre pas

```bash
# Voir les logs du service en échec
docker compose logs airflow-scheduler --tail 100
docker compose logs postgres-data --tail 50

# Redémarrer un service spécifique
docker compose restart airflow-scheduler
```

Causes fréquentes :
- **`airflow-init` pas terminé** : attendre que le conteneur `airflow-init` sorte avec code 0 avant `docker compose up -d`
- **Port déjà occupé** : `lsof -i :8080` ou `lsof -i :5433` pour identifier le processus concurrent
- **Clé Fernet manquante** : vérifier que `AIRFLOW__CORE__FERNET_KEY` est bien dans `.env`

---

### Erreur "DAG not found" après démarrage

Le scheduler met 30 à 60 secondes à charger les DAGs. Vérifier d'abord :

```bash
# Erreurs de parsing des DAGs
docker compose exec airflow-apiserver airflow dags list-import-errors
```

Causes fréquentes :
- Dépendance Python manquante : vérifier `requirements.txt` et relancer `docker compose build`
- Erreur de syntaxe dans un fichier DAG : le message d'erreur indique le fichier et la ligne

---

### Impossible de se connecter à la base métier

```bash
# Tester la connexion depuis l'intérieur du réseau Docker
docker compose exec airflow-apiserver python -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook('postgres_data')
print(hook.get_first('SELECT 1'))
"
```

Si erreur `connection refused` : vérifier que `postgres-data` est `healthy` :
```bash
docker compose ps postgres-data
```

Si erreur `password authentication failed` : vérifier `PG_DATA_PASSWORD` dans `.env` et la connexion `postgres_data` dans Airflow.

---

### L'API AMUE renvoie 401

La connexion `oauth_api` ne contient pas les bons credentials.

```bash
# Tester l'authentification OAuth directement
curl -s -X POST "https://sandbox.auth.amue.fr/auth/fer/oauth/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=<votre_id>" \
  -d "client_secret=<votre_secret>"
```

Mettre à jour la connexion si les credentials ont changé :
```bash
docker compose exec airflow-apiserver airflow connections delete oauth_api
# Puis recréer avec les bons credentials (voir Étape 6)
```

---

### Emails non reçus en développement

Les emails sont capturés par MailHog (pas envoyés réellement). Les consulter sur http://localhost:8025.

Si MailHog n'est pas accessible : vérifier que le service tourne (`docker compose ps mailhog`) et que le port 8025 n'est pas bloqué par un firewall.

---

## Fichiers clés

| Fichier                           | Rôle                                              |
|-----------------------------------|---------------------------------------------------|
| `.env`                            | Variables Docker (générées depuis `.env.example`) |
| `docker-compose.yml`              | Définition des 10 services                        |
| `scripts/sql/init_db.sql`         | Création des schémas et tables admin              |
| `config/airflow_variables.json`   | Variables Airflow (import en masse)               |
| `config/airflow_connections.json` | Structure des connexions (sans credentials)       |
| `scripts/install/quick_setup.sh`  | Script d'installation interactif                  |
| `manage.sh`                       | CLI principal de gestion                          |
