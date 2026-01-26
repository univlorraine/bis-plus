# Airflow AMUE - Système d'Import de Données

Système d'import automatisé de données depuis l'API AMUE vers PostgreSQL via Apache Airflow.

## Prérequis

- **Docker** et **Docker Compose**
- **jq** (parsing JSON)
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
4. Tables à importer

**Interfaces :**
- Airflow UI : http://localhost:8080 (airflow/airflow)
- MailHog : http://localhost:8025 (emails de test)

## Architecture

```
dags/
└── dag_amue_dynamic_table.py      # DAG principal

plugins/amue/
├── hooks/
│   └── amue_api_hook.py           # Communication OAuth API
├── operators/
│   ├── data_importer.py           # Import données avec pagination
│   ├── table_filter.py            # Sélection des tables
│   ├── table_manager.py           # Gestion DDL PostgreSQL
│   └── table_verifier.py          # Vérification structure
├── services/
│   ├── metadata_manager.py        # Gestion fingerprints
│   ├── polling_service.py         # Polling avec backoff
│   ├── retry_service.py           # Retry intelligent
│   └── status_checker.py          # Vérification statuts API
├── notifications/
│   ├── email_service.py           # Service SMTP
│   ├── notification_service.py    # Orchestration notifications
│   ├── report_generator.py        # Rapports d'exécution
│   ├── templates/                 # Templates HTML emails
│   └── notifiers/                 # Notifiers erreur/succès
└── utils/
    ├── airflow_helpers.py         # Gestion variables Airflow
    ├── hooks.py                   # Gestionnaire hooks (singleton)
    ├── settings.py                # Configuration (dataclass)
    └── transformers.py            # Conversion types SQL

config/
├── airflow_variables.json         # Variables Airflow
├── airflow_connections.json       # Connexions Airflow
└── log_config.py                  # Configuration logging

tests/                             # Tests unitaires (pytest)
```

## Workflow du DAG

```
PHASE 1 : INITIALISATION
┌─────────────────────────────────────────────────────────────┐
│ wait_for_api_and_select()                                   │
│   • Polling jusqu'à disponibilité de l'API                  │
│   • Sélection des tables configurées                        │
└─────────────────────────────────────────────────────────────┘
                              ↓
PHASE 2 : VÉRIFICATION (parallèle par table)
┌─────────────────────────────────────────────────────────────┐
│ verify_table.expand()                                       │
│   • Vérifie statut + structure + fingerprint                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ validate_tables()                                           │
│   • Agrège les résultats, arrête si erreurs                 │
└─────────────────────────────────────────────────────────────┘
                              ↓
PHASE 3 : IMPORT (parallèle par table)
┌─────────────────────────────────────────────────────────────┐
│ prepare_table.expand()                                      │
│   • Prépare structure PostgreSQL (dev uniquement)           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ import_data.expand()                                        │
│   • Import avec pagination, INSERT ou UPSERT                │
└─────────────────────────────────────────────────────────────┘
                              ↓
PHASE 4 : FINALISATION
┌─────────────────────────────────────────────────────────────┐
│ save_metadata() → send_report()                             │
│   • Mise à jour fingerprints + envoi rapport email          │
└─────────────────────────────────────────────────────────────┘
```

## Fonctionnalités

### Import de données
- Import complet ou différentiel (colonne delta)
- Jusqu'à 10 tables en parallèle
- Pagination automatique des grands volumes
- UPSERT si clé primaire définie
- Activation/désactivation individuelle des tables (`enable`)

### Retry intelligent
Stratégies adaptées selon le type d'erreur :

| Code    | Stratégie                         |
|---------|-----------------------------------|
| 4xx     | Pas de retry (erreur client)      |
| 429     | Retry agressif avec backoff court |
| 5xx     | Backoff exponentiel standard      |
| Timeout | Retry court, peu de tentatives    |

### Notifications
- Emails HTML responsive (succès et erreur)
- Contexte d'erreur détaillé avec actions recommandées
- MailHog intégré pour les tests

### Contrôles de production
- Vérification statut API avant import
- Détection changements de structure (fingerprint)
- Création de tables interdite en production
- Rollback automatique en cas d'erreur

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
./manage.sh setup              # Installation complète
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
  "amue_tables_to_import": [
    {"name": "CSKS", "enable": true, "primary_key": "", "delta": "", "last_import": "", "finger_print": ""},
    {"name": "COVP", "enable": true, "primary_key": "", "delta": "", "last_import": "", "finger_print": ""},
    {"name": "EKET", "enable": false, "primary_key": "", "delta": "bedat", "last_import": "", "finger_print": ""}
  ],
  "amue_import_batch_size": "5000",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "smtp_host": "mailhog",
  "smtp_port": "1025",
  "amue_report_recipients": "admin@example.com"
}
```

**Attributs des tables :**
| Attribut | Description |
|----------|-------------|
| `name` | Nom de la table (obligatoire) |
| `enable` | Active/désactive la table (défaut: `true`) |
| `primary_key` | Clés primaires pour UPSERT (ex: `"BUKRS,KOSTL"`) |
| `delta` | Colonne de date pour import différentiel |
| `last_import` | Date ISO du dernier import |
| `finger_print` | Empreinte de structure (auto-générée) |

### Sécurité des credentials

| Fichier | Contenu | Git |
|---------|---------|-----|
| `.env` | Credentials (secrets) | **Non** |
| `.env.example` | Template | Oui |
| `config/*.json` | Configuration (sans secrets) | Oui |

## Tests

```bash
# Via manage.sh (recommandé)
./manage.sh tests              # Tous les tests
./manage.sh tests-cov          # Avec couverture

# Via pytest
pytest tests/ -v
pytest --cov=plugins/amue --cov-report=html
```

12 fichiers de tests couvrent : API hook, import, métadonnées, notifications, polling, retry, configuration, filtrage, gestion tables, transformations SQL.

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

### Création de tables
En production, les tables doivent être créées manuellement :
```sql
CREATE TABLE ma_table (
    id INTEGER PRIMARY KEY,
    ...
);
```

## Technologies

- Apache Airflow 3.1.3
- PostgreSQL 15
- Python 3.12
- Docker & Docker Compose
- pytest
