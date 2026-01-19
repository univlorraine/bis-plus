# Airflow AMUE - Système d'Import de Données Automatisé

## Description

Système complet d'import automatisé de données depuis l'API AMUE vers PostgreSQL via Apache Airflow. Ce projet fournit une solution robuste, maintenable et production-ready pour l'intégration de données financières universitaires.

## Nouveautés de la Version Refactorisée

### Améliorations Architecturales

- **Séparation des Responsabilités** : Chaque classe a une responsabilité unique et claire
- **Dataclasses** : Utilisation de `@dataclass` pour des structures de données typées et immuables
- **Gestion d'Erreurs Améliorée** : Messages d'erreur détaillés et contextuels
- **Logging Enrichi** : Logs structurés avec progression et métriques

### Nouvelles Fonctionnalités

#### 1. Service de Polling Intelligent (`AMUEPollingService`)
- **Backoff Exponentiel** (optionnel) : Augmentation progressive des intervalles d'attente
- **Affichage de Progression** : Indicateurs visuels pour les attentes longues
- **Détection d'Erreurs Critiques** : Arrêt immédiat sur codes 4xx (sauf 429)
- **Métriques Détaillées** : Temps écoulé, nombre de tentatives, derniers codes HTTP

#### 2. Gestionnaire de Tables Robuste (`AMUETableManager`)
- **Validation Stricte** : Vérification de la complétude des structures avant opération
- **Contrôle Environnemental** : Interdiction absolue de création en production
- **Messages d'Erreur Explicites** : Diagnostics détaillés en cas de problème
- **SQL Formaté** : Génération de DDL lisible et maintenable

#### 3. Système de Notifications Modulaire
- **Architecture en couches** : EmailService, Templates, Notifiers
- **Templates HTML réutilisables** : Base commune avec styles partagés
- **Notifiers spécialisés** : ErrorNotifier, SuccessNotifier
- **Emails HTML Responsive** : Design moderne avec CSS Grid
- **Contexte d'Erreur Enrichi** : Type d'erreur, stack trace, actions recommandées
- **Échappement HTML Sécurisé** : Protection contre l'injection
- **Liens Directs** : Accès rapide à Airflow UI depuis l'email

#### 4. Gestionnaire de Métadonnées Amélioré (`AMUEMetadataManager`)
- **API Flexible** : Récupération et réinitialisation de métadonnées par table
- **Gestion SDK/API** : Support Airflow 2.x et 3.x avec fallback automatique
- **Validation Robuste** : Vérification des types et structures
- **Logs Détaillés** : Suivi précis des changements de fingerprint

#### 5. Configuration Centralisée (`AMUEConfig`)
- **Dataclass Typée** : Configuration validée avec types stricts
- **Chargement Automatique** : Depuis les variables Airflow avec `get_config()`
- **Singleton Pattern** : Instance unique réutilisable
- **Rechargement à Chaud** : Via `reload_config()` pour rafraîchir les paramètres
- **Validation des Variables Requises** : Erreurs explicites si variables manquantes

#### 6. Gestionnaire de Hooks (`HookManager`)
- **Singleton Pattern** : Réutilisation des connexions
- **Lazy Loading** : Création des hooks à la demande
- **Hooks Intégrés** : API AMUE et PostgreSQL préconfigurés

#### 7. Callback Airflow (`send_failure_notification`)
- **Intégration Native** : Callback compatible `on_failure_callback`
- **Extraction Automatique** : Récupération du contexte d'erreur depuis Airflow
- **Notifications Immédiates** : Envoi d'email dès qu'une tâche échoue

#### 8. Configuration Logging Personnalisée
- **Réduction du Bruit** : Filtrage des logs verbeux (dagbag, dag_processing)
- **Configuration Externe** : Via `config/log_config.py`
- **Compatible Airflow 3.x** : Utilise `deep_update` pour merger la configuration

## Prérequis

- **Docker** et **Docker Compose** installés
- **jq** installé (pour le parsing JSON)
- Au moins **4GB de RAM** disponible
- Ports disponibles : **8080** (Airflow), **5432/5433** (PostgreSQL), **8025** (MailHog)

## Installation Rapide

```bash
# 1. Cloner le projet
git clone <votre-repo>
cd airflow-amue

# 2. Rendre les scripts exécutables
chmod +x manage.sh scripts/**/*.sh

# 3. Lancer l'installation interactive
./manage.sh setup
```

Le script d'installation vous demandera :
1. **Environnement** : `dev` (sandbox) ou `prod` (production)
2. **Credentials AMUE** : Client ID et Secret (stockés dans `.env` uniquement)
3. **Credentials PostgreSQL** : Login et password (stockés dans `.env` uniquement)
4. **Tables à importer** : Liste des tables AMUE

**Accès à l'interface** : http://localhost:8080
- Username: `airflow`
- Password: `airflow`

**Interface emails (dev)** : http://localhost:8025

### Sécurité des Credentials

Les credentials ne sont **jamais** stockés dans les fichiers de configuration JSON.
Ils sont uniquement dans le fichier `.env` qui est exclu du versioning.

| Fichier | Contenu | Commitable |
|---------|---------|------------|
| `.env` | Credentials (login, password, secrets) | **Non** |
| `.env.example` | Template avec placeholders | Oui |
| `config/airflow_connections.json` | Structure des connexions (hosts, ports) | Oui |
| `config/airflow_variables.json` | Configuration métier | Oui |

## Caractéristiques Principales

### Fonctionnalités d'Import
- **Import Complet** : Import initial de toutes les données d'une table
- **Import Différentiel** : Mise à jour incrémentale basée sur une colonne delta
- **Multi-tables** : Import de plusieurs tables en parallèle
- **Vérification Historique** : Contrôle du statut des N derniers jours
- **Retry Automatique** : Gestion intelligente des erreurs avec retry
- **Pagination Automatique** : Gestion transparente des grands volumes de données

### Contrôles de Production
- **Vérification du statut de l'API** avant import
- **Détection automatique** des changements de structure
- **Validation des clés primaires**
- **Contrôles différenciés** selon l'environnement (dev/prod)
- **Rollback automatique** en cas d'erreur
- **Empreinte digitale** (fingerprint) des structures de tables

### Notifications et Reporting
- **Emails de succès** avec rapport détaillé (HTML moderne)
- **Emails d'erreur** avec diagnostic et actions recommandées
- **Serveur SMTP de test** intégré (MailHog) pour développement
- **Rapports HTML formatés** avec tableaux de bord
- **Métriques d'exécution** : temps, tentatives, lignes importées

### Automatisation et Monitoring
- **Installation en une commande**
- **Configuration depuis fichiers JSON**
- **Scripts de diagnostic automatique**
- **Tests automatisés**
- **Correction automatique des problèmes**
- **Interface web de monitoring** (Airflow UI)
- **Interface de visualisation des emails** (MailHog)

## Architecture

### Architecture Logicielle

Le projet suit les **principes SOLID** avec une séparation claire des responsabilités :

```
config/
└── log_config.py                          # Configuration logging Airflow

dags/
└── dag_amue_dynamic_table.py              # DAG principal (orchestration)

plugins/amue/
├── __init__.py                            # Exports centralisés
├── hooks/
│   └── amue_api_hook.py                   # Communication OAuth API
├── operators/
│   ├── data_importer.py                   # Import données
│   ├── table_filter.py                    # Filtrage tables
│   ├── table_manager.py                   # Gestion DDL PostgreSQL
│   └── table_verifier.py                  # Vérification structure
├── services/
│   ├── metadata_manager.py                # Gestion métadonnées
│   ├── polling_service.py                 # Service de polling
│   └── status_checker.py                  # Vérification statuts
├── notifications/
│   ├── email_service.py                   # Service SMTP générique
│   ├── notification_service.py            # Service de notification centralisé
│   ├── report_generator.py                # Rapports d'exécution
│   ├── templates/
│   │   ├── base.py                        # Template de base (styles)
│   │   ├── error.py                       # Template erreur
│   │   └── success.py                     # Template succès
│   └── notifiers/
│       ├── base.py                        # Classe abstraite
│       ├── error_notifier.py              # Notifications d'erreur
│       └── success_notifier.py            # Notifications de succès
└── utils/
    ├── airflow_helpers.py                 # Gestion variables Airflow
    ├── hooks.py                           # Gestionnaire de hooks (singleton)
    ├── settings.py                        # Configuration centralisée (AMUEConfig)
    └── transformers.py                    # Fonctions utilitaires
```

### Classes Principales

| Classe | Responsabilité |
|--------|---------------|
| `AMUEAPIHook` | Communication OAuth avec l'API AMUE |
| `AMUEStatusChecker` | Vérification des statuts historiques et actuels |
| `AMUETableFilter` | Filtrage et sélection des tables à traiter |
| `AMUETableVerifier` | Vérification structure et statut des tables |
| `AMUETableManager` | Création et gestion des structures PostgreSQL |
| `AMUEDataImporter` | Import avec pagination, retry et UPSERT |
| `AMUEPollingService` | Attente de disponibilité de l'API avec backoff |
| `AMUEMetadataManager` | Gestion des empreintes et dates d'import |
| `AMUEReportGenerator` | Génération rapports et envoi emails |
| `AMUEConfig` | Configuration centralisée (dataclass singleton) |
| `HookManager` | Gestionnaire centralisé de hooks (singleton) |
| `EmailService` | Service SMTP générique |
| `NotificationService` | Service de notification centralisé |
| `ErrorNotifier` | Notifications d'erreur |
| `SuccessNotifier` | Notifications de succès |
| `ErrorContext` | Dataclass pour le contexte d'erreur |

### Workflow du DAG

```
1. check_historical_status()
   Vérifie les statuts historiques (N jours)
   ↓
2. wait_for_ready()
   Polling intelligent avec backoff et progression
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
   Validation stricte + création sécurisée (dev uniquement)
   ↓
7. import_table_data()
   Import parallèle avec INSERT ou UPSERT
   ↓
8. update_metadata()
   Mise à jour fingerprints avec API flexible
   ↓
9. generate_report()
   Génération rapport d'exécution
   ↓
10. send_notification()
    Envoi email avec HTML moderne et contexte riche
```

## Configuration

### Variables Airflow Nouvelles/Modifiées

#### Configuration du Polling (Nouveau)

```json
{
  "amue_polling_exponential_backoff": "true",
  "amue_polling_max_backoff_minutes": "60"
}
```

- **`amue_polling_exponential_backoff`** (boolean) : Active le backoff exponentiel
  - `false` (défaut) : Intervalle fixe
  - `true` : Augmentation progressive (2^n * interval)

- **`amue_polling_max_backoff_minutes`** (int) : Temps maximum entre deux tentatives
  - Défaut : `60` minutes
  - Limite supérieure du backoff exponentiel

#### Variables Existantes

```json
{
  "environment": "dev|production",
  "oauth_api_connection_id": "oauth_api",
  "universite": "ul",
  "api_endpoint_admin": "finances/cdv/v1/preprod/${univ}/admin",
  "api_endpoint_table": "finances/cdv/v1/preprod/${univ}/table",
  "amue_max_history_days": "7",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "2",
  "amue_report_recipients": "email1@domain.com,email2@domain.com",
  "smtp_host": "mailhog",
  "smtp_port": "1025",
  "smtp_mail_from": "airflow@amue-project.local"
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
./manage.sh diagnose           # Diagnostic complet du système
./manage.sh test-config        # Test rapide (30 secondes)
```

## Développement

### Exemples d'Utilisation des Nouvelles API

#### 1. Utilisation du Service de Polling avec Backoff

```python
from utils.amue_polling_service import AMUEPollingService, PollingConfig

# Configuration avec backoff exponentiel
config = PollingConfig(
    interval_minutes=5,
    max_wait_hours=2,
    exponential_backoff=True,
    max_backoff_minutes=30
)

polling_service = AMUEPollingService(status_checker, config)
result = polling_service.wait_for_ready()

print(f"Prêt après {result['attempts']} tentatives")
print(f"Temps total: {result['total_wait_minutes']:.1f} minutes")
```

#### 2. Récupération de Métadonnées d'une Table

```python
from utils.amue_metadata_manager import AMUEMetadataManager

manager = AMUEMetadataManager()

# Récupère les métadonnées
metadata = manager.get_table_metadata('CSKS')
if metadata:
    print(f"Dernier import: {metadata.last_import}")
    print(f"Fingerprint: {metadata.finger_print}")

# Réinitialise les métadonnées
manager.reset_table_metadata('CSKS')
```

#### 3. Envoi de Notification d'Erreur

```python
from amue import ErrorNotifier

notifier = ErrorNotifier()
notifier.notify({
    'dag_id': 'mon_dag',
    'task_id': 'ma_tache',
    'error_message': 'Erreur de connexion',
    'error_type': 'ConnectionError'
})
```

#### 4. Envoi de Notification de Succès

```python
from amue import SuccessNotifier

notifier = SuccessNotifier()
notifier.notify({
    'dag_id': 'mon_dag',
    'tables_imported': [
        {'table_name': 'CSKS', 'rows_inserted': 1000, 'status': 'success'},
        {'table_name': 'CEPC', 'rows_inserted': 500, 'status': 'success'}
    ],
    'duration': '5m 30s'
})
```

#### 5. Envoi d'Email Générique

```python
from amue.notifications import EmailService, Email

service = EmailService()
email = Email(
    to=['user@example.com'],
    subject='Test',
    html_content='<h1>Hello</h1>'
)
service.send(email)
```

#### 6. Utilisation de la Configuration Centralisée

```python
from amue import get_config, reload_config, AMUEConfig

# Récupérer la configuration (singleton)
config = get_config()

print(f"Environnement: {config.environment}")
print(f"Université: {config.universite}")
print(f"Production: {config.is_production()}")
print(f"Max retries: {config.api_max_retries}")

# Forcer le rechargement après modification des variables
config = reload_config()
```

#### 7. Utilisation du Gestionnaire de Hooks

```python
from amue import HookManager

# Singleton - réutilise les connexions
hooks = HookManager()

# Hook API AMUE (lazy loading)
api_hook = hooks.api_hook
response = api_hook.get_table_status('CSKS')

# Hook PostgreSQL (lazy loading)
pg_hook = hooks.postgres_hook
records = pg_hook.get_records("SELECT * FROM splus.csks LIMIT 10")
```

#### 8. Callback d'Erreur Airflow

```python
from amue import send_failure_notification

# Dans la définition du DAG
with DAG(
    'mon_dag',
    default_args={
        'on_failure_callback': send_failure_notification
    }
) as dag:
    # Les tâches enverront automatiquement un email en cas d'échec
    ...

# Ou au niveau d'une tâche spécifique
task = PythonOperator(
    task_id='ma_tache',
    python_callable=ma_fonction,
    on_failure_callback=send_failure_notification
)
```

### Tests

```bash
# Test rapide de configuration
./manage.sh test-config

# Test de la configuration email
./manage.sh test-email

# Diagnostic complet
./manage.sh diagnose
```

## Production

### Checklist de Déploiement

#### Sécurité
- [ ] Changer tous les mots de passe par défaut
- [ ] Configurer un serveur SMTP réel
- [ ] Activer HTTPS sur l'interface web
- [ ] Configurer les secrets Docker ou variables d'environnement
- [ ] Restreindre l'accès réseau avec firewall

#### Configuration
- [ ] Modifier `environment` à `production`
- [ ] Configurer les vraies credentials API
- [ ] Définir les bons destinataires d'emails
- [ ] Configurer le serveur SMTP de production
- [ ] Adapter `amue_tables_to_import` selon les besoins
- [ ] Configurer le backoff exponentiel si nécessaire

#### Infrastructure
- [ ] Provisionner les ressources (4GB RAM minimum)
- [ ] Configurer les backups automatiques
- [ ] Mettre en place le monitoring
- [ ] Configurer les alertes

## Résolution de Problèmes

### Problèmes Courants

#### Polling Trop Long

**Symptôme** : Le polling prend trop de temps avec intervalle fixe

**Solution** :
```bash
# Activer le backoff exponentiel
./manage.sh config
# Modifier amue_polling_exponential_backoff à "true"
```

#### Erreurs de Structure en Production

**Symptôme** : `Table inexistante. Création interdite en production`

**Solution** :
```sql
-- Créer manuellement la table en production
CREATE TABLE ma_table (
    id INTEGER PRIMARY KEY,
    ...
);
```

#### Emails Non Reçus

**Symptôme** : Emails d'erreur non reçus

**Solution** :
```bash
# Vérifier le service SMTP
./manage.sh logs mailhog

# Tester l'envoi
./manage.sh test-email

# Vérifier dans http://localhost:8025
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

## Statistiques du Projet

| Métrique | Valeur |
|----------|--------|
| Lignes de code Python | ~5500 |
| Lignes de scripts Bash | ~2000 |
| Lignes de documentation | ~9000 |
| Nombre de classes | 16 |
| Nombre de scripts | 12 |
| **Dataclasses** | **5** |
| **API publiques** | **12** |

## Améliorations Futures

- [ ] Support des webhooks pour notifications temps réel
- [ ] Intégration avec Prometheus pour métriques
- [ ] Dashboard Grafana personnalisé
- [ ] Support multi-environnements (dev/staging/prod)
- [ ] API REST pour gestion externe
- [ ] Tests unitaires complets
- [ ] Documentation API avec Swagger

## Licence

Ce projet est un outil interne. Consultez votre organisation pour les détails de licence.

## Crédits

Développé pour l'intégration des données financières universitaires depuis l'API AMUE.

**Technologies utilisées** :
- Apache Airflow 3.1.3
- PostgreSQL 15
- Python 3.12
- Docker & Docker Compose
- MailHog (développement)

**Principes de conception** :
- SOLID
- Clean Architecture
- Type Hints (PEP 484)
- Dataclasses (PEP 557)
- Logging structuré