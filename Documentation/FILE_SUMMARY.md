# 📁 Résumé des Fichiers du Projet

## Structure Complète

```
airflow-amue/
├── dags/
│   ├── amue_multi_table_import_refactored.py    # DAG principal refactorisé
│   └── utils/
│       ├── __init__.py                          # Exports du package
│       ├── amue_api_hook.py                     # Hook API OAuth
│       ├── amue_utils.py                        # Fonctions utilitaires
│       ├── amue_status_checker.py               # Vérification statuts
│       ├── amue_table_filter.py                 # Filtrage tables
│       ├── amue_table_verifier.py               # Vérification structure
│       ├── amue_table_manager.py                # Gestion structure PostgreSQL
│       ├── amue_data_importer.py                # Import données
│       ├── amue_polling_service.py              # Service polling
│       ├── amue_metadata_manager.py             # Gestion métadonnées
│       └── amue_report_generator.py             # Rapports et notifications
│
├── config/
│   ├── airflow_variables.json                   # Variables Airflow
│   ├── airflow_connections.json                 # Connexions Airflow
│   └── exports/                                 # Exports de configuration
│
├── scripts/
│   ├── setup_airflow_config.sh                  # Configuration Airflow
│   ├── quick_setup.sh                           # Setup automatique complet
│   └── init-db.sql                              # Initialisation PostgreSQL
│
├── logs/                                        # Logs Airflow (généré)
├── plugins/                                     # Plugins Airflow (vide)
├── backups/                                     # Backups BDD (généré)
│
├── manage.sh                                    # Script de gestion principal
├── docker-compose.yml                           # Configuration Docker Compose
├── .env                                         # Variables d'environnement
├── .env.example                                 # Template .env
├── .gitignore                                   # Exclusions Git
│
├── QUICK_START.md                               # Guide démarrage rapide
├── SETUP_GUIDE.md                               # Guide installation détaillé
├── REFACTORING_DOCUMENTATION.md                 # Documentation architecture
└── FILES_SUMMARY.md                             # Ce fichier
```

---

## 📄 Description des Fichiers

### 🎯 Fichiers Principaux

#### `manage.sh`
**Script de gestion centralisé**
- Commandes pour gérer les services Docker
- Opérations sur les DAGs
- Gestion de la configuration
- Opérations sur la base de données
- Outils de développement

**Usage**: `./manage.sh [COMMAND]`

#### `docker-compose.yml`
**Configuration Docker Compose**
- Définit tous les services (Airflow, PostgreSQL)
- Configure les volumes et réseaux
- Définit les healthchecks
- Gère l'initialisation automatique

---

### 🔧 Configuration

#### `config/airflow_variables.json`
**Variables Airflow**
```json
{
  "environment": "production",
  "oauth_api_connection_id": "oauth_api",
  "amue_tables_to_import": [...],
  ...
}
```
- Configuration globale du DAG
- Paramètres de polling et retry
- Liste des tables à importer
- Destinataires des notifications

#### `config/airflow_connections.json`
**Connexions Airflow**
```json
{
  "oauth_api": {...},
  "postgres_data": {...}
}
```
- Connexion API AMUE (OAuth)
- Connexion PostgreSQL données

⚠️ **IMPORTANT**: Ne pas commiter avec vraies credentials!

---

### 🤖 Scripts d'Automatisation

#### `scripts/setup_airflow_config.sh`
**Configuration Airflow depuis fichiers JSON**
- Mode interne (dans le container)
- Mode externe (via docker-compose exec)
- Vérification de la configuration
- Export de la configuration

**Modes**:
- `--internal` : Configuration interne
- `--external` : Configuration externe (défaut)
- `--verify` : Vérification
- `--export` : Export

#### `scripts/quick_setup.sh`
**Setup automatique complet**
1. Vérifie les prérequis
2. Crée la structure de dossiers
3. Génère les fichiers de configuration
4. Démarre Docker Compose
5. Configure Airflow automatiquement
6. Vérifie l'installation

**Usage**: `./scripts/quick_setup.sh`

#### `scripts/init-db.sql`
**Initialisation PostgreSQL**
- Crée le schéma `splus`
- Configure les permissions
- Exécuté automatiquement au premier démarrage

---

### 🐍 Code Python - DAG

#### `dags/amue_multi_table_import_refactored.py`
**DAG principal refactorisé**
- Orchestration des tasks
- Utilise les classes du package utils
- Architecture propre et maintenable
- ~200 lignes (vs 800+ avant)

**Workflow**:
1. Vérification historique
2. Polling disponibilité API
3. Filtrage des tables
4. Vérifications parallèles (statut + structure)
5. Gestion des structures
6. Import des données (parallèle)
7. Mise à jour métadonnées
8. Génération rapport + notification

---

### 🔨 Code Python - Classes Utils

#### `dags/utils/__init__.py`
**Package utils**
- Exporte toutes les classes
- Point d'entrée centralisé

#### `dags/utils/amue_status_checker.py`
**AMUEStatusChecker**
- Vérification statuts historiques
- Récupération statut actuel
- Vérification code HTTP (polling)

**Méthodes**:
- `check_historical_status(max_days)` → Dict
- `get_current_status()` → Dict
- `check_status_code()` → int

#### `dags/utils/amue_table_filter.py`
**AMUETableFilter**
- Filtrage des tables à traiter
- Vérification historique par table
- Détermination type d'import

**Méthodes**:
- `filter_tables(current_status, history)` → List[Dict]

#### `dags/utils/amue_table_verifier.py`
**AMUETableVerifier**
- Vérification statut table
- Vérification structure
- Détection changements

**Méthodes**:
- `verify_status(table_info)` → Dict
- `verify_structure(table_info)` → Dict

#### `dags/utils/amue_table_manager.py`
**AMUETableManager**
- Gestion structure PostgreSQL
- Création tables (dev uniquement)
- Validation tables existantes

**Méthodes**:
- `manage_table(structure_info)` → Dict

#### `dags/utils/amue_data_importer.py`
**AMUEDataImporter**
- Import données avec pagination
- Retry automatique
- INSERT ou UPSERT selon contexte

**Méthodes**:
- `import_table(table_name, columns, primary_keys, import_config)` → Dict

#### `dags/utils/amue_polling_service.py`
**AMUEPollingService**
- Polling disponibilité API
- Retry avec délai configurable
- Timeout configurable

**Méthodes**:
- `wait_for_ready()` → Dict

#### `dags/utils/amue_metadata_manager.py`
**AMUEMetadataManager**
- Mise à jour fingerprints
- Mise à jour dates derniers imports
- Sauvegarde dans Variables Airflow

**Méthodes**:
- `update_metadata(import_results)` → None

#### `dags/utils/amue_report_generator.py`
**AMUEReportGenerator**
- Génération rapports d'exécution
- Envoi notifications email
- Logs structurés

**Méthodes**:
- `generate_report(...)` → Dict
- `send_notification(report)` → None

---

### 📚 Documentation

#### `QUICK_START.md`
**Guide de démarrage rapide**
- Installation en 3 minutes
- Commandes essentielles
- Résolution problèmes courants
- Checklist post-installation

#### `SETUP_GUIDE.md`
**Guide d'installation détaillé**
- Prérequis complets
- Installation manuelle
- Configuration avancée
- Monitoring et sécurité
- Dépannage complet

#### `REFACTORING_DOCUMENTATION.md`
**Documentation de l'architecture**
- Principes de refactoring
- Description des classes
- Patterns utilisés
- Exemples d'utilisation
- Tests recommandés

#### `FILES_SUMMARY.md`
**Ce fichier**
- Vue d'ensemble de la structure
- Description de chaque fichier
- Arborescence complète

---

### ⚙️ Configuration Environnement

#### `.env`
**Variables d'environnement**
```bash
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```
⚠️ Ne pas commiter (dans .gitignore)

#### `.env.example`
**Template .env**
- Copier vers `.env`
- Modifier selon besoins
- Commitable (pas de secrets)

#### `.gitignore`
**Exclusions Git**
- Fichiers sensibles (.env, connections.json)
- Logs
- Fichiers Python compilés
- Dossiers temporaires

---

## 🔄 Flux de Configuration

### Installation Initiale

```
1. Cloner le repo
   ↓
2. chmod +x manage.sh scripts/*.sh
   ↓
3. Modifier config/airflow_connections.json (credentials)
   ↓
4. ./manage.sh setup
   ↓
5. Accéder à http://localhost:8080
```

### Modification de Configuration

```
1. Modifier config/airflow_variables.json
   ↓
2. ./manage.sh config
   ↓
3. Vérifier: ./manage.sh verify
```

### Développement

```
1. Modifier le code dans dags/
   ↓
2. Les changements sont détectés automatiquement
   ↓
3. Vérifier logs: ./manage.sh logs dag-processor
   ↓
4. Tester: ./manage.sh test amue_multi_table_import
```

---

## 📊 Tailles Approximatives

| Fichier | Lignes | Taille |
|---------|--------|--------|
| manage.sh | ~450 | ~15 KB |
| setup_airflow_config.sh | ~400 | ~14 KB |
| quick_setup.sh | ~350 | ~12 KB |
| amue_multi_table_import_refactored.py | ~200 | ~8 KB |
| Chaque classe utils/* | ~150-250 | ~6-10 KB |
| docker-compose.yml | ~250 | ~9 KB |
| SETUP_GUIDE.md | ~500 | ~20 KB |

**Total Code**: ~3500 lignes
**Total Documentation**: ~2000 lignes

---

## 🎯 Fichiers à Personnaliser

### ✏️ TOUJOURS Modifier

1. **config/airflow_connections.json**
   - Credentials API AMUE
   - Mots de passe PostgreSQL

2. **config/airflow_variables.json**
   - Liste des tables à importer
   - Emails de notification
   - Paramètres selon votre environnement

3. **.env**
   - Mots de passe Airflow UI
   - AIRFLOW_UID selon votre système

### 🔧 Parfois Modifier

4. **docker-compose.yml**
   - Ports si conflits
   - Volumes additionnels
   - Resources limits

5. **scripts/init-db.sql**
   - Schémas additionnels
   - Tables de configuration

### 📖 Jamais Modifier (sauf pour développement)

- Scripts bash (manage.sh, setup_*)
- Classes Python utils/*
- Documentation *.md

---

## 🔐 Fichiers Sensibles

### ⚠️ Ne JAMAIS Commiter

```
.env
config/airflow_connections.json (avec vraies credentials)
config/exports/*
logs/*
backups/*
```

### ✅ Commitable

```
.env.example
config/airflow_variables.json (sans données sensibles)
tous les fichiers *.sh
tous les fichiers *.py
tous les fichiers *.md
docker-compose.yml
.gitignore
```

---

## 🚀 Quick Reference

```bash
# Setup initial
./manage.sh setup

# Opérations quotidiennes
./manage.sh start
./manage.sh status
./manage.sh logs scheduler
./manage.sh trigger amue_multi_table_import

# Maintenance
./manage.sh db-backup
./manage.sh clean
./manage.sh verify

# Développement
./manage.sh shell
./manage.sh test <dag_id>
./manage.sh python
```

---

**Dernière mise à jour**: 2025-01-19
**Version**: 2.0 (Refactorisée)