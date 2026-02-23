# Airflow AMUE - Système d'Import de Données

Système d'import automatisé de données depuis l'API AMUE vers PostgreSQL via Apache Airflow, avec architecture Blue/Green pour des déploiements atomiques et rollback instantané.

## Prérequis

- **Docker** et **Docker Compose**
- **jq** (parsing JSON)
- **psql** (client PostgreSQL pour le setup Blue/Green)
- **4GB RAM** minimum
- Ports disponibles : **8080** (Airflow), **5432/5433** (PostgreSQL), **8025** (MailHog)

## Installation

```bash
# Cloner et installer
git clone <votre-repo>
cd airflow-amue
chmod +x manage.sh scripts/**/*.sh
./manage.sh setup
```

Le script demande :
1. Environnement (`dev` ou `production`)
2. Credentials API AMUE (Client ID, Secret)
3. Credentials PostgreSQL

Le setup crée automatiquement les schémas Blue/Green (`splus`, `splus_blue`, `splus_green`) et le schéma d'administration (`splus_admin`).

**Interfaces :**
- Airflow UI : http://localhost:8080 (airflow/airflow)
- MailHog : http://localhost:8025 (emails de test)

## Architecture

```
dags/
├── dag_amue_dynamic_table.py      # DAG principal d'import
└── dag_amue_sync.py               # DAG de synchronisation Blue/Green

plugins/amue/
├── operators/
│   ├── pipeline/
│   │   ├── data_importer.py       # Import données avec pagination
│   │   ├── data_streamer.py       # Streaming données API
│   │   ├── batch_inserter.py      # Insertion batch dans schéma cible
│   │   └── duplicate_detector.py  # Détection doublons
│   └── table_management/
│       ├── table_filter.py        # Sélection et filtrage des tables
│       ├── table_manager.py       # Gestion DDL
│       └── table_verifier.py      # Vérification structure + fingerprint
├── services/
│   ├── bluegreen/
│   │   ├── bluegreen_manager.py   # Gestion état Blue/Green
│   │   ├── view_switcher.py       # Switch atomique des vues
│   │   ├── schema_synchronizer.py # Synchronisation des schémas
│   │   └── rollback_manager.py    # Rollback vers état précédent
│   ├── api/
│   │   ├── polling_service.py     # Polling avec backoff
│   │   └── status_checker.py      # Vérification statuts API
│   ├── metadata_manager.py        # Gestion fingerprints et timestamps
│   ├── admin_state_manager.py     # Accès à splus_admin.amue_state
│   ├── table_config_manager.py    # Accès à splus_admin.amue_tables
│   └── retry_service.py           # Retry intelligent par type d'erreur
├── tasks/
│   ├── import_dag/                # 9 fonctions @task du DAG principal
│   ├── rollback_dag/              # Fonctions @task du DAG rollback
│   └── sync_dag/                  # Fonctions @task du DAG sync
├── notifications/
│   ├── email_service.py           # Service SMTP
│   ├── notifier.py                # Orchestration notifications
│   ├── report_generator.py        # Rapports d'exécution
│   ├── callbacks.py               # Callbacks Airflow
│   └── templates/                 # Templates HTML emails
└── utils/
    ├── config/
    │   ├── settings.py            # Configuration (dataclass)
    │   └── airflow_helpers.py     # Gestion variables Airflow
    ├── database/
    │   ├── hooks.py               # Gestionnaire hooks (singleton)
    │   ├── connection_manager.py  # Gestion connexions
    │   └── schema_utils.py        # Utilitaires schémas
    ├── transformers.py            # Conversion types SQLite→PostgreSQL
    └── tracing.py                 # Traçage des opérations

config/
├── airflow_variables.json         # Variables Airflow
├── airflow_connections.json       # Connexions Airflow
└── log_config.py                  # Configuration logging

scripts/sql/
├── init_db.sql                    # Initialisation BDD + Blue/Green
├── init_admin_schema.sql          # Création schéma splus_admin
├── create_bluegreen_schemas.sql   # Création schémas Blue/Green
└── migrate_to_bluegreen.sql       # Migration tables existantes

tests/                             # 539 tests unitaires (pytest)
```

## Architecture Blue/Green

L'architecture Blue/Green permet des imports atomiques avec possibilité de rollback instantané.

### Principe

```
PostgreSQL Database
├── splus_blue/           # Tables blue (données)
│   ├── csks, prps, ...   # 32+ tables
│
├── splus_green/          # Tables green (identiques)
│   ├── csks, prps, ...
│
├── splus/                # Vues (interface publique)
│   ├── csks → VIEW vers splus_blue.csks OU splus_green.csks
│   └── ...
│
└── splus_admin/          # Administration (état, config tables)
    ├── amue_state        # État du DAG (singleton)
    └── amue_tables       # Configuration et fingerprints par table
```

### Stratégie

```
┌─────────────────────────────────────────────────────────────────┐
│  DAG Run N                                                       │
│  ─────────                                                       │
│  1. Actif: BLUE, Cible: GREEN                                   │
│  2. Import nouvelles données → GREEN                            │
│  3. Switch vues → GREEN devient actif                           │
│  4. BLUE reste intact = SNAPSHOT pour rollback                  │
│                                                                  │
│  ⏳ Période de rollback disponible (jusqu'au prochain DAG)      │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│  DAG Run N+1                                                     │
│  ───────────                                                     │
│  1. Actif: GREEN, Cible: BLUE                                   │
│  2. Import nouvelles données → BLUE                             │
│  3. Switch vues → BLUE devient actif                            │
│  4. GREEN reste intact = SNAPSHOT pour rollback                 │
└─────────────────────────────────────────────────────────────────┘
```

### Rollback

Si des données incorrectes sont détectées après un import réussi :

```bash
# Rollback instantané via DAG dédié
./manage.sh trigger amue_rollback
```

Le rollback bascule simplement les vues vers le schéma inactif (< 1 seconde).

### Activation

Le mode Blue/Green est contrôlé par la variable Airflow :
```json
{
  "amue_bluegreen_enabled": "true"
}
```

## Workflow du DAG

```
PHASE 1 : INITIALISATION BLUE/GREEN
┌─────────────────────────────────────────────────────────────────┐
│ init_bluegreen()                                                │
│   • Détermine le schéma cible (opposé de l'actif)               │
│   • Acquiert le verrou d'import (atomique via PostgreSQL)       │
│   • Retourne target_schema (splus_blue ou splus_green)          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
PHASE 2 : POLLING & SÉLECTION
┌─────────────────────────────────────────────────────────────────┐
│ wait_for_api_and_select()                                       │
│   • Polling jusqu'à disponibilité d'un nouveau rapport API      │
│   • Sélection des tables configurées (enable=true) depuis BDD   │
│   • Injection du target_schema dans chaque table                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
PHASE 3 : VÉRIFICATION (parallèle par table)
┌─────────────────────────────────────────────────────────────────┐
│ verify_table.expand()                                           │
│   • Vérifie statut + structure + fingerprints                   │
│   • Vérifie dans le schéma cible                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ validate_tables()                                               │
│   • Agrège les résultats, arrête si erreurs                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
PHASE 4 : IMPORT (parallèle par table)
┌─────────────────────────────────────────────────────────────────┐
│ prepare_table.expand()                                          │
│   • Prépare table dans schéma cible (splus_blue/green)          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ import_data.expand()                                            │
│   • Import avec pagination, UPSERT (INSERT ON CONFLICT UPDATE)  │
│   • Écrit dans le schéma cible                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
PHASE 5 : SWITCH & FINALISATION
┌─────────────────────────────────────────────────────────────────┐
│ save_metadata()                                                 │
│   • Mise à jour fingerprints (fingerprint_api, fingerprint_ul)  │
│   • Enregistrement des timestamps (finish, report_start)        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ switch_views()                                                  │
│   • Bascule atomique des vues vers schéma cible                 │
│   • Crée les vues manquantes (DROP + CREATE)                    │
│   • Active le rollback vers l'ancien schéma                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ send_report()                                                   │
│   • Envoi rapport email avec résultats                          │
└─────────────────────────────────────────────────────────────────┘
```

## Fonctionnalités

### Import de données
- Import complet (FULL) ou différentiel (colonne delta)
- Jusqu'à 10 tables en parallèle
- Pagination automatique des grands volumes
- UPSERT pour tous les modes (INSERT ON CONFLICT UPDATE — jamais de suppression)
- Colonnes de traçage `_source` et `_imported_at` sur toutes les tables
- Activation/désactivation individuelle des tables (`enable`)

### Architecture Blue/Green
- Switch atomique des vues (toutes ou aucune)
- Vues gérées uniquement lors du switch (schémas `splus_blue`/`splus_green` jamais renommés)
- Rollback instantané vers l'état précédent
- Verrou d'import atomique via PostgreSQL (sans race condition)

### Retry intelligent
Stratégies adaptées selon le type d'erreur :

| Code       | Tentatives | Stratégie                                    |
|------------|------------|----------------------------------------------|
| 4xx        | 0          | Pas de retry (erreur client)                 |
| 429        | 5          | Backoff exponentiel 2s→4s→8s→16s→30s + jitter |
| 5xx        | 3          | Backoff exponentiel 5s→10s→20s + jitter      |
| Timeout    | 2          | Délai fixe 3s                                |
| Connexion  | 3          | Backoff exponentiel 5s→10s→20s + jitter      |

### Notifications
- Emails HTML responsive (succès et erreur)
- Contexte d'erreur détaillé avec actions recommandées
- MailHog intégré pour les tests

### Contrôles de production
- Vérification statut API avant import
- Détection changements de structure (double fingerprint)
- Rollback automatique en cas d'erreur SQL

## Commandes

### Services
```bash
./manage.sh start              # Démarre les services
./manage.sh stop               # Arrête les services
./manage.sh restart            # Redémarre
./manage.sh status             # État des services
./manage.sh logs [service]     # Logs
./manage.sh health             # Santé des composants
```

### Configuration
```bash
./manage.sh setup              # Installation complète (inclut Blue/Green)
./manage.sh setup-bluegreen    # Initialise uniquement les schémas Blue/Green
./manage.sh config             # Reconfigure depuis JSON
./manage.sh verify             # Vérifie la configuration
./manage.sh diagnose           # Diagnostic complet
./manage.sh config-validate    # Valide .env, variables, connexions
./manage.sh config-backup      # Sauvegarde configuration
./manage.sh config-restore <f> # Restaure depuis archive
```

### DAGs
```bash
./manage.sh dags               # Liste les DAGs
./manage.sh trigger [dag_id]   # Déclenche un DAG
./manage.sh pause [dag_id]     # Met en pause
./manage.sh unpause [dag_id]   # Réactive
./manage.sh backfill [dag_id]  # Relance exécutions passées
```

### Monitoring
```bash
./manage.sh failed [limit]     # Tâches en échec
./manage.sh task-logs [dag] [task] [run]  # Logs d'une tâche
./manage.sh validate           # Valide syntaxe DAGs
./manage.sh lint               # Analyse code DAGs
```

### Variables Airflow
```bash
./manage.sh variables          # Liste les variables
./manage.sh var-get <key>      # Affiche une variable
./manage.sh var-set <key> [val]# Définit une variable
./manage.sh var-delete <key>   # Supprime une variable
./manage.sh var-export [file]  # Exporte en JSON
./manage.sh var-import <file>  # Importe depuis JSON
```

### Connexions Airflow
```bash
./manage.sh connections        # Liste les connexions
./manage.sh conn-test [name]   # Teste les connexions
./manage.sh conn-export        # Exporte (secrets masqués)
./manage.sh conn-update <name> # Met à jour une connexion
```

### Tables
```bash
./manage.sh list-tables              # Liste les tables avec statut enable
./manage.sh add-table [t1 t2..]      # Ajoute une ou plusieurs tables
./manage.sh remove-table <t1 t2..>   # Supprime une ou plusieurs tables
./manage.sh toggle-table <t1 t2..>   # Bascule enable d'une ou plusieurs tables
./manage.sh enable-table <t1 t2..>   # Active une ou plusieurs tables
./manage.sh disable-table <t1 t2..>  # Désactive une ou plusieurs tables
```

### Base de données
```bash
./manage.sh db-shell           # Shell PostgreSQL
./manage.sh db-backup          # Sauvegarde
./manage.sh db-restore [file]  # Restaure
```

### Maintenance
```bash
./manage.sh cleanup-logs [days]# Supprime vieux logs
./manage.sh cleanup-db [days]  # Purge anciennes exécutions
./manage.sh reset              # Reset complet
./manage.sh clean              # Nettoie fichiers temporaires
```

### Développement
```bash
./manage.sh tests [file]       # Lance les tests pytest
./manage.sh tests-cov          # Tests avec couverture
./manage.sh test-email         # Test configuration email
./manage.sh shell              # Shell dans le container
./manage.sh python             # Console Python
```

## Configuration

### Variables Airflow principales

```json
{
  "environment": "dev",
  "oauth_api_connection_id": "oauth_api",
  "universite": "ul",
  "amue_import_batch_size": "5000",
  "amue_polling_interval_minutes": "10",
  "amue_polling_max_backoff_minutes": "60",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_default_source": "sifac_plus",
  "amue_bluegreen_enabled": "true",
  "amue_import_schedule": "0 2 * * *",
  "amue_sync_schedule": "0 6 * * *",
  "smtp_host": "mailhog",
  "smtp_port": "1025",
  "amue_report_recipients": "admin@example.com"
}
```

### Configuration des tables (`splus_admin.amue_tables`)

La liste des tables et leur configuration sont stockées dans la table PostgreSQL `splus_admin.amue_tables`. Chaque ligne correspond à une table AMUE :

```sql
SELECT table_name, enabled, primary_key, delta, fingerprint_api, fingerprint_ul
FROM splus_admin.amue_tables;
```

| Colonne          | Description                                                         |
|------------------|---------------------------------------------------------------------|
| `table_name`     | Nom de la table (obligatoire)                                       |
| `enabled`        | Active/désactive la table (défaut: `true`)                          |
| `primary_key`    | Clés primaires pour UPSERT, prioritaires sur l'API (ex: `BUKRS,KOSTL`) |
| `delta`          | Colonne de date pour import différentiel                            |
| `fingerprint_api`| Empreinte de structure côté API (auto-générée)                     |
| `fingerprint_ul` | Empreinte de structure côté PostgreSQL (auto-générée)              |

### Variables Blue/Green

| Variable                 | Description                                       |
|--------------------------|---------------------------------------------------|
| `amue_bluegreen_enabled` | Active le mode Blue/Green (`"true"` ou `"false"`) |

### État Blue/Green (`splus_admin.amue_state`)

L'état opérationnel est stocké dans la table PostgreSQL `splus_admin.amue_state` (ligne unique) :

```sql
SELECT * FROM splus_admin.amue_state WHERE id = 1;
```

| Colonne                | Description                                              |
|------------------------|----------------------------------------------------------|
| `last_finish_timestamp`| Timestamp finish du dernier rapport AMUE traité          |
| `last_report_start`    | Timestamp start du dernier rapport (référence delta)     |
| `last_successful_run`  | Horodatage du dernier import réussi                      |
| `import_in_progress`   | Import en cours                                          |
| `import_started_at`    | Début de l'import en cours                               |
| `import_correlation_id`| Identifiant de corrélation de l'import                   |
| `last_switch_timestamp`| Dernier switch Blue/Green                                |
| `last_sync_timestamp`  | Dernière synchronisation Blue/Green                      |

### Sécurité des credentials

| Fichier         | Contenu                      | Git     |
|-----------------|------------------------------|---------|
| `.env`          | Credentials (secrets)        | **Non** |
| `.env.example`  | Template                     | Oui     |
| `config/*.json` | Configuration (sans secrets) | Oui     |

## Tests

```bash
# Via manage.sh (recommandé)
./manage.sh tests              # Tous les tests (539 tests)
./manage.sh tests-cov          # Avec couverture

# Via pytest
pytest tests/ -v
pytest --cov=plugins/amue --cov-report=html
```

Les tests couvrent :
- API hook, import, métadonnées, notifications
- Polling, retry, configuration, filtrage
- Gestion tables, transformations SQL
- **Blue/Green** : bluegreen_manager, view_switcher, schema_synchronizer, rollback_manager
- **Services admin** : admin_state_manager, table_config_manager

## Résolution de problèmes

### Configuration invalide
```bash
./manage.sh config-validate    # Identifier les problèmes
./manage.sh config             # Reconfigurer
```

### Connexions défaillantes
```bash
./manage.sh conn-test          # Tester les connexions
./manage.sh conn-update <name> # Mettre à jour
```

### Emails non reçus
```bash
./manage.sh test-email         # Tester l'envoi
./manage.sh logs mailhog       # Vérifier MailHog
# Ouvrir http://localhost:8025
```

### Problèmes Blue/Green
```bash
# Vérifier l'état en BDD
psql -c "SELECT import_in_progress, last_finish_timestamp FROM splus_admin.amue_state;"

# Réinitialiser les schémas
./manage.sh setup-bluegreen

# Rollback manuel
./manage.sh trigger amue_rollback
```

### Diagnostic complet
```bash
./manage.sh diagnose > diagnostic.log
./manage.sh health
```

## Production

### Checklist
- [ ] Changer mots de passe par défaut
- [ ] Configurer serveur SMTP réel
- [ ] Définir `environment` à `production`
- [ ] Configurer credentials API réels
- [ ] Définir destinataires emails
- [ ] Configurer backups automatiques
- [ ] Activer HTTPS
- [ ] Activer Blue/Green (`amue_bluegreen_enabled: "true"`)

### Création de tables
En production, les tables doivent être créées manuellement dans les deux schémas :
```sql
-- Créer dans splus_blue
CREATE TABLE splus_blue.ma_table (
    id INTEGER PRIMARY KEY,
    ...
    _source VARCHAR(50) DEFAULT 'sifac_plus',
    _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Créer dans splus_green (identique)
CREATE TABLE splus_green.ma_table (
    id INTEGER PRIMARY KEY,
    ...
    _source VARCHAR(50) DEFAULT 'sifac_plus',
    _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- La vue sera créée automatiquement lors du premier switch
```

### Migration vers Blue/Green

Pour migrer des tables existantes :
```bash
# Exécuter le script de migration
psql -h $HOST -U $USER -d $DB -f scripts/sql/migrate_to_bluegreen.sql
```

## Technologies

- Apache Airflow 3.1.3
- PostgreSQL 15
- Python 3.12
- Docker & Docker Compose
- pytest (539 tests)
