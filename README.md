# Airflow AMUE - Système d'Import de Données Automatisé

## Description

Système complet d'import automatisé de données depuis l'API AMUE vers PostgreSQL via Apache Airflow. Ce projet fournit une solution robuste, maintenable et production-ready pour l'intégration de données financières universitaires.

## Caractéristiques Principales

### Fonctionnalités d'Import

- **Import Complet** : Import initial de toutes les données d'une table
- **Import Différentiel** : Mise à jour incrémentale basée sur une colonne delta
- **Multi-tables** : Import de plusieurs tables en parallèle
- **Vérification Historique** : Contrôle du statut des N derniers jours
- **Retry Automatique** : Gestion intelligente des erreurs avec retry
- **Pagination Automatique** : Gestion transparente des grands volumes de données

### Contrôles de Production

- Vérification du statut de l'API avant import
- Détection automatique des changements de structure
- Validation des clés primaires
- Contrôles différenciés selon l'environnement (dev/prod)
- Rollback automatique en cas d'erreur
- Empreinte digitale (fingerprint) des structures de tables

### Notifications et Reporting

- Emails de succès avec rapport détaillé
- Emails d'erreur avec diagnostic
- Serveur SMTP de test intégré (MailHog) pour développement
- Support SMTP réel pour production
- Rapports HTML formatés avec tableaux de bord

### Automatisation et Monitoring

- Installation en une commande
- Configuration depuis fichiers JSON
- Scripts de diagnostic automatique
- Tests automatisés
- Correction automatique des problèmes
- Interface web de monitoring (Airflow UI)
- Interface de visualisation des emails (MailHog)

## Architecture

### Architecture Logicielle

Le projet suit les principes SOLID avec une séparation claire des responsabilités :

```
dags/
├── amue_multi_table_import_refactored.py  # DAG principal (orchestration)
└── utils/                                  # Packages métier
    ├── __init__.py
    ├── amue_api_hook.py                   # Communication OAuth API
    ├── amue_status_checker.py             # Vérification statuts
    ├── amue_table_filter.py               # Filtrage tables
    ├── amue_table_verifier.py             # Vérification structure
    ├── amue_table_manager.py              # Gestion DDL PostgreSQL
    ├── amue_data_importer.py              # Import données
    ├── amue_polling_service.py            # Service de polling
    ├── amue_metadata_manager.py           # Gestion métadonnées
    ├── amue_report_generator.py           # Rapports et notifications
    └── amue_utils.py                      # Fonctions utilitaires
```

### Classes Principales

| Classe | Responsabilité | Lignes |
|--------|----------------|--------|
| `AMUEAPIHook` | Communication OAuth avec l'API AMUE | 150 |
| `AMUEStatusChecker` | Vérification des statuts historiques et actuels | 180 |
| `AMUETableFilter` | Filtrage et sélection des tables à traiter | 150 |
| `AMUETableVerifier` | Vérification structure et statut des tables | 250 |
| `AMUETableManager` | Création et gestion des structures PostgreSQL | 120 |
| `AMUEDataImporter` | Import avec pagination, retry et UPSERT | 280 |
| `AMUEPollingService` | Attente de disponibilité de l'API | 80 |
| `AMUEMetadataManager` | Gestion des empreintes et dates d'import | 100 |
| `AMUEReportGenerator` | Génération rapports et envoi emails | 180 |

### Workflow du DAG

```
1. check_historical_status()
   Vérifie les statuts historiques (N jours)
   ↓
2. wait_for_update_ready()
   Polling avec retry jusqu'à disponibilité API
   ↓
3. filter_tables_to_process()
   Filtre les tables selon statut et historique
   ↓
4. verify_table_status() + verify_table_structure()
   Vérifications parallèles du statut et de la structure
   ↓
5. combine_verifications()
   Agrégation des résultats, arrêt si erreurs
   ↓
6. manage_table_structure()
   Création tables (dev) ou validation (prod)
   ↓
7. import_table_data()
   Import parallèle avec INSERT ou UPSERT
   ↓
8. update_metadata()
   Mise à jour empreintes et dates
   ↓
9. generate_report()
   Génération rapport d'exécution
   ↓
10. send_notification()
    Envoi email de notification
```

## Architecture Infrastructure

### Services Docker

```yaml
services:
  postgres              # Base métadonnées Airflow
  postgres-data         # Base données métier (schéma splus)
  airflow-apiserver     # API et interface web
  airflow-scheduler     # Ordonnanceur
  airflow-dag-processor # Processeur de DAGs
  airflow-triggerer     # Déclencheur de tâches
  mailhog              # Serveur SMTP de test (dev)
```

### Ports Exposés

| Port | Service | Description |
|------|---------|-------------|
| 8080 | Airflow UI | Interface web de gestion |
| 8025 | MailHog UI | Interface de visualisation des emails |
| 5432 | PostgreSQL Airflow | Base métadonnées (interne) |
| 5433 | PostgreSQL Data | Base données métier (externe) |
| 1025 | MailHog SMTP | Serveur SMTP de test (interne) |

### Volumes Persistants

- `postgres-db-volume` : Métadonnées Airflow
- `postgres-data-volume` : Données métier
- `./dags` : DAGs et code Python
- `./logs` : Logs d'exécution
- `./config` : Fichiers de configuration
- `./scripts` : Scripts d'automatisation

## Configuration

### Variables Airflow

Fichier `config/airflow_variables.json` :

```json
{
  "environment": "dev|production",
  "oauth_api_connection_id": "oauth_api",
  "univerite": "ul",
  "api_endpoint_admin": "finances/cdv/v1/preprod/${univ}/admin",
  "api_endpoint": "finances/cdv/v1/preprod/${univ}/table",
  "amue_max_history_days": "7",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "2",
  "amue_report_recipients": "email1@domain.com,email2@domain.com",
  "smtp_host": "mailhog",
  "smtp_port": "1025",
  "smtp_mail_from": "airflow@amue-project.local",
  "amue_tables_to_import": [...],
  "amue_last_successful_run": "",
  "last_import_report": ""
}
```

#### Variables Principales

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `environment` | Environnement d'exécution | `dev` |
| `univerite` | Code université (substitution ${univ}) | `ul` |
| `api_endpoint_admin` | Endpoint API admin avec variable | `finances/cdv/v1/preprod/${univ}/admin` |
| `api_endpoint` | Endpoint API données avec variable | `finances/cdv/v1/preprod/${univ}/table` |
| `amue_max_history_days` | Jours d'historique à vérifier | `7` |
| `amue_polling_interval_minutes` | Intervalle de polling (minutes) | `10` |
| `amue_max_wait_hours` | Temps maximum d'attente (heures) | `6` |
| `amue_api_max_retries` | Nombre de tentatives par requête | `3` |
| `amue_report_recipients` | Destinataires notifications (CSV) | - |
| `smtp_host` | Serveur SMTP | `mailhog` |
| `smtp_port` | Port SMTP | `1025` |

#### Configuration des Tables

Structure de `amue_tables_to_import` :

```json
{
  "name": "NOM_TABLE",
  "primary_key": "COL1,COL2",
  "delta": "COLONNE_DATE",
  "last_import": "2026-01-05T10:04:29.607130",
  "finger_print": "hash_md5"
}
```

- `name` : Nom de la table (requis)
- `primary_key` : Clés primaires séparées par virgules (optionnel)
- `delta` : Colonne de date pour import différentiel (optionnel)
- `last_import` : Date dernier import (géré automatiquement)
- `finger_print` : Empreinte MD5 de la structure (géré automatiquement)

### Connexions Airflow

Fichier `config/airflow_connections.json` :

```json
{
  "oauth_api": {
    "conn_type": "http",
    "host": "https://sandbox.api.amue.fr",
    "login": "client_id",
    "password": "client_secret",
    "extra": {
      "token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token",
      "api_base_url": "https://sandbox.api.amue.fr"
    }
  },
  "postgres_data": {
    "conn_type": "postgres",
    "host": "postgres-data",
    "schema": "business_data",
    "login": "datauser",
    "password": "datapass",
    "port": 5432
  }
}
```

## Utilisation

### Script de Gestion Centralisé

Le script `manage.sh` centralise toutes les opérations :

```bash
./manage.sh COMMANDE [OPTIONS]
```

#### Gestion des Services

```bash
./manage.sh start              # Démarre tous les services
./manage.sh stop               # Arrête tous les services
./manage.sh restart            # Redémarre tous les services
./manage.sh status             # Affiche l'état des services
./manage.sh logs [service]     # Affiche les logs
```

#### Configuration

```bash
./manage.sh setup              # Installation complète automatique
./manage.sh config             # Reconfigure depuis les fichiers JSON
./manage.sh fix                # Correction avec attente API
./manage.sh auto-fix           # Détection et correction automatique
./manage.sh verify             # Vérifie la configuration
./manage.sh verify-email       # Vérifie le correctif email
./manage.sh diagnose           # Diagnostic complet du système
./manage.sh test-config        # Test rapide (30 secondes)
```

#### DAGs

```bash
./manage.sh dags                    # Liste tous les DAGs
./manage.sh trigger <dag_id>        # Déclenche un DAG
./manage.sh pause <dag_id>          # Met en pause un DAG
./manage.sh unpause <dag_id>        # Réactive un DAG
```

#### Base de Données

```bash
./manage.sh db-shell            # Shell PostgreSQL interactif
./manage.sh db-backup           # Sauvegarde la base de données
./manage.sh db-restore <file>   # Restaure une sauvegarde
```

#### Développement

```bash
./manage.sh test <dag_id>       # Test un DAG
./manage.sh test-email          # Test configuration email
./manage.sh shell               # Shell bash interactif
./manage.sh python              # Console Python interactive
./manage.sh clean               # Nettoie les fichiers temporaires
```

### Accès aux Interfaces Web

#### Airflow UI

- **URL** : http://localhost:8080
- **Identifiants** : `airflow` / `airflow`
- **Fonctionnalités** :
  - Gestion des DAGs (activation, déclenchement, pause)
  - Visualisation du graphe d'exécution
  - Consultation des logs par tâche
  - Gestion des variables et connexions
  - Historique des exécutions

#### MailHog UI (Développement)

- **URL** : http://localhost:8025
- **Fonctionnalités** :
  - Visualisation de tous les emails capturés
  - Lecture du contenu HTML
  - Consultation des headers
  - Téléchargement des emails
  - API REST pour intégration

## Développement

### Structure du Code

Le code suit les principes SOLID :

- **S**ingle Responsibility : Chaque classe a une responsabilité unique
- **O**pen/Closed : Ouvert à l'extension, fermé à la modification
- **L**iskov Substitution : Les classes dérivées sont substituables
- **I**nterface Segregation : Interfaces spécifiques plutôt que générales
- **D**ependency Inversion : Dépendance aux abstractions

### Ajout d'une Nouvelle Table

1. Éditer `config/airflow_variables.json`
2. Ajouter dans `amue_tables_to_import` :
```json
{
  "name": "MA_NOUVELLE_TABLE",
  "primary_key": "ID_COLONNE",
  "delta": "DATE_MAJ",
  "last_import": "",
  "finger_print": ""
}
```
3. Appliquer la configuration :
```bash
./manage.sh fix
```

### Modification d'une Classe

Exemple pour ajouter une fonctionnalité à `AMUEDataImporter` :

1. Éditer `dags/utils/amue_data_importer.py`
2. Ajouter votre méthode
3. Mettre à jour les tests si nécessaire
4. Redémarrer les services :
```bash
./manage.sh restart
```

### Tests

```bash
# Test rapide de configuration
./manage.sh test-config

# Test de la configuration email
./manage.sh test-email

# Diagnostic complet
./manage.sh diagnose

# Test d'un DAG spécifique
./manage.sh test amue_multi_table_import_v2
```

## Production

### Checklist de Déploiement

#### Sécurité

- [ ] Changer tous les mots de passe par défaut
- [ ] Configurer un serveur SMTP réel
- [ ] Activer HTTPS sur l'interface web
- [ ] Configurer les secrets Docker ou variables d'environnement
- [ ] Restreindre l'accès réseau avec firewall
- [ ] Configurer l'authentification LDAP/SSO si nécessaire

#### Configuration

- [ ] Modifier `environment` à `production`
- [ ] Configurer les vraies credentials API
- [ ] Définir les bons destinataires d'emails
- [ ] Configurer le serveur SMTP de production
- [ ] Adapter `amue_tables_to_import` selon les besoins
- [ ] Définir les clés primaires de toutes les tables

#### Infrastructure

- [ ] Provisionner les ressources (4GB RAM minimum)
- [ ] Configurer les backups automatiques
- [ ] Mettre en place le monitoring
- [ ] Configurer les alertes
- [ ] Définir la politique de rétention des logs

#### Tests

- [ ] Exécuter `./manage.sh diagnose`
- [ ] Tester l'envoi d'emails
- [ ] Exécuter un import de test
- [ ] Vérifier les logs
- [ ] Valider les données importées

### Configuration SMTP Production

#### Gmail

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_STARTTLS: 'True'
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
  AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_APP_PASSWORD}
```

#### Office 365

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: smtp.office365.com
  AIRFLOW__SMTP__SMTP_STARTTLS: 'True'
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
  AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
```

### Backup et Restauration

```bash
# Backup automatique quotidien
./manage.sh db-backup

# Les backups sont dans backups/business_data_YYYYMMDD_HHMMSS.sql

# Restauration
./manage.sh db-restore backups/business_data_20260106_120000.sql
```

### Monitoring

#### Via Logs

```bash
# Logs en temps réel
./manage.sh logs scheduler

# Recherche d'erreurs
./manage.sh logs scheduler | grep -i error

# Logs d'un service spécifique
./manage.sh logs airflow-apiserver
```

#### Via Airflow UI

- DAGs : Vue d'ensemble des exécutions
- Graph View : Visualisation du workflow
- Gantt : Timeline d'exécution
- Task Duration : Performances
- Code : Inspection du code source

## Résolution de Problèmes

### Problèmes Courants

#### Configuration Non Appliquée

**Symptôme** : Variables ou connexions non créées

**Solution** :
```bash
./manage.sh auto-fix
```

#### Emails Non Envoyés

**Symptôme** : Erreur SMTP ou emails non reçus

**Solution** :
```bash
./manage.sh test-email
# Vérifier dans http://localhost:8025
```

#### Services Ne Démarrent Pas

**Symptôme** : Containers en état "Exited"

**Solution** :
```bash
./manage.sh stop
docker-compose down -v  # ATTENTION : Supprime les données
./manage.sh start
```

#### DAG Ne S'Affiche Pas

**Symptôme** : DAG absent de l'interface

**Solution** :
```bash
# Vérifier les erreurs de parsing
docker-compose exec airflow-apiserver airflow dags list-import-errors

# Redémarrer le processeur de DAGs
docker-compose restart airflow-dag-processor
```

### Diagnostic

```bash
# Diagnostic complet du système
./manage.sh diagnose > diagnostic.log

# Le fichier diagnostic.log contient :
# - État des services
# - Configuration SMTP
# - Variables et connexions
# - DAGs détectés
# - Erreurs récentes
```

## Support et Contribution

### Documentation Complète

Ce projet inclut une documentation exhaustive :

- `README.md` : Vue d'ensemble (ce fichier)
- `INSTALL.md` : Guide d'installation détaillé
- `config/` : Exemples de configuration
- `scripts/` : Scripts avec commentaires inline

### Logs

Tous les logs sont disponibles dans `./logs/` et via :
```bash
./manage.sh logs [service]
```

### Statistiques du Projet

- **Lignes de code Python** : ~4000
- **Lignes de scripts Bash** : ~2000
- **Lignes de documentation** : ~6000
- **Nombre de classes** : 9
- **Nombre de scripts** : 12
- **Temps d'installation** : 5 minutes
- **Temps de configuration** : 2 minutes

## Licence

Ce projet est un outil interne. Consultez votre organisation pour les détails de licence.

## Crédits

Développé pour l'intégration des données financières universitaires depuis l'API AMUE.

Technologies utilisées :
- Apache Airflow 3.1.3
- PostgreSQL 15
- Python 3.12
- Docker & Docker Compose
- MailHog (développement)