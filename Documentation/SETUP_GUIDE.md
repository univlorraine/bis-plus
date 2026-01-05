# Guide de Configuration Airflow AMUE

Ce guide explique comment configurer et déployer l'environnement Airflow pour l'import des données AMUE.

## 📋 Prérequis

- Docker et Docker Compose installés
- `jq` installé (pour le parsing JSON)
- Au moins 4GB de RAM disponible
- Ports 8080, 5432, 5433 disponibles

## 🚀 Installation Rapide

### Option 1: Setup Automatique (Recommandé)

```bash
# Rendre le script exécutable
chmod +x scripts/quick_setup.sh

# Lancer le setup complet
./scripts/quick_setup.sh
```

Ce script va :
1. Vérifier les prérequis
2. Créer la structure de dossiers
3. Générer les fichiers de configuration par défaut
4. Démarrer les containers Docker
5. Configurer automatiquement Airflow
6. Vérifier l'installation

### Option 2: Setup Manuel

#### 1. Créer les fichiers de configuration

Créez `config/airflow_variables.json`:
```json
{
  "environment": "production",
  "oauth_api_connection_id": "oauth_api",
  "api_endpoint_status": "finances/cdv/v1/preprod/ul/admin",
  "api_endpoint_admin": "finances/cdv/v1/preprod/ul/admin",
  "api_endpoint": "finances/cdv/v1/preprod/ul/table",
  "amue_max_history_days": "7",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "30",
  "amue_report_recipients": "admin@example.com",
  "amue_tables_to_import": [
    {
      "name": "CSKS",
      "primary_key": "CODE_STRUCTURE",
      "delta": "DATE_MAJ",
      "last_import": "",
      "finger_print": ""
    }
  ]
}
```

Créez `config/airflow_connections.json`:
```json
{
  "oauth_api": {
    "conn_type": "http",
    "host": "https://api.amue.fr",
    "login": "your_client_id",
    "password": "your_client_secret",
    "extra": {
      "token_url": "https://oauth.amue.fr/token",
      "grant_type": "client_credentials"
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

#### 2. Démarrer les containers

```bash
docker-compose up -d
```

#### 3. Attendre que les services soient prêts

```bash
# Vérifier les logs
docker-compose logs -f airflow-init

# Attendre environ 2-3 minutes
```

#### 4. Configurer Airflow

```bash
# Rendre le script exécutable
chmod +x scripts/setup_airflow_config.sh

# Lancer la configuration
./scripts/setup_airflow_config.sh --external
```

## 📁 Structure du Projet

```
.
├── dags/
│   ├── amue_multi_table_import_refactored.py
│   └── utils/
│       ├── __init__.py
│       ├── amue_api_hook.py
│       ├── amue_status_checker.py
│       ├── amue_table_filter.py
│       ├── amue_table_verifier.py
│       ├── amue_table_manager.py
│       ├── amue_data_importer.py
│       ├── amue_polling_service.py
│       ├── amue_metadata_manager.py
│       ├── amue_report_generator.py
│       └── amue_utils.py
├── config/
│   ├── airflow_variables.json
│   ├── airflow_connections.json
│   └── exports/                    # Exports de configuration
├── scripts/
│   ├── quick_setup.sh             # Setup automatique
│   ├── setup_airflow_config.sh    # Configuration Airflow
│   └── init-db.sql                # Initialisation PostgreSQL
├── logs/                          # Logs Airflow
├── plugins/                       # Plugins Airflow
├── docker-compose.yml
├── .env
└── README.md
```

## 🔧 Configuration des Variables

### Variables Principales

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `environment` | Environnement (dev/production) | `production` |
| `oauth_api_connection_id` | ID de la connexion OAuth | `oauth_api` |
| `amue_max_history_days` | Jours d'historique à vérifier | `7` |
| `amue_polling_interval_minutes` | Intervalle de polling (minutes) | `10` |
| `amue_max_wait_hours` | Temps max d'attente (heures) | `6` |
| `amue_api_max_retries` | Nombre de tentatives | `3` |
| `amue_report_recipients` | Emails pour notifications | `admin@example.com` |

### Configuration des Tables

Le paramètre `amue_tables_to_import` est un tableau JSON contenant les tables à importer:

```json
{
  "name": "NOM_TABLE",           // Nom de la table
  "primary_key": "COL1,COL2",    // Clés primaires (virgule)
  "delta": "DATE_MAJ",           // Colonne pour import différentiel
  "last_import": "",             // Date dernier import (auto)
  "finger_print": ""             // Hash structure (auto)
}
```

## 🔌 Configuration des Connexions

### Connexion OAuth API

```json
{
  "oauth_api": {
    "conn_type": "http",
    "host": "https://api.amue.fr",
    "login": "CLIENT_ID",
    "password": "CLIENT_SECRET",
    "extra": {
      "token_url": "https://oauth.amue.fr/token",
      "grant_type": "client_credentials",
      "scope": "api_read"
    }
  }
}
```

### Connexion PostgreSQL Données

```json
{
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

## 🛠️ Commandes Utiles

### Script de Configuration

```bash
# Configuration depuis l'extérieur du container (recommandé)
./scripts/setup_airflow_config.sh --external

# Configuration depuis l'intérieur du container
./scripts/setup_airflow_config.sh --internal

# Vérifier la configuration actuelle
./scripts/setup_airflow_config.sh --verify

# Exporter la configuration actuelle
./scripts/setup_airflow_config.sh --export

# Aide
./scripts/setup_airflow_config.sh --help
```

### Docker Compose

```bash
# Démarrer tous les services
docker-compose up -d

# Arrêter tous les services
docker-compose down

# Voir les logs en temps réel
docker-compose logs -f airflow-scheduler

# Redémarrer un service
docker-compose restart airflow-scheduler

# Exécuter une commande dans un container
docker-compose exec airflow-apiserver airflow variables list
```

### Airflow CLI

```bash
# Lister les variables
docker-compose exec airflow-apiserver airflow variables list

# Voir une variable
docker-compose exec airflow-apiserver airflow variables get amue_tables_to_import

# Modifier une variable
docker-compose exec airflow-apiserver airflow variables set environment "development"

# Lister les connexions
docker-compose exec airflow-apiserver airflow connections list

# Tester une connexion
docker-compose exec airflow-apiserver airflow connections test postgres_data

# Lister les DAGs
docker-compose exec airflow-apiserver airflow dags list

# Déclencher un DAG
docker-compose exec airflow-apiserver airflow dags trigger amue_multi_table_import_v2
```

## 🔄 Mise à Jour de la Configuration

### Après modification des fichiers JSON

1. Modifiez `config/airflow_variables.json` ou `config/airflow_connections.json`
2. Relancez la configuration:
   ```bash
   ./scripts/setup_airflow_config.sh --external
   ```

### Via l'interface Web

1. Accédez à http://localhost:8080
2. Allez dans Admin > Variables ou Admin > Connections
3. Modifiez directement dans l'interface

### Export de la configuration actuelle

Pour sauvegarder la configuration actuelle:

```bash
./scripts/setup_airflow_config.sh --export
```

Les fichiers seront exportés dans `config/exports/` avec un timestamp.

## 🐛 Dépannage

### Les containers ne démarrent pas

```bash
# Vérifier les logs
docker-compose logs airflow-init

# Vérifier les ressources
docker stats

# Nettoyer et redémarrer
docker-compose down -v
docker-compose up -d
```

### La configuration ne s'applique pas

```bash
# Vérifier que jq est installé
jq --version

# Vérifier que les fichiers JSON sont valides
jq . config/airflow_variables.json
jq . config/airflow_connections.json

# Relancer la configuration
./scripts/setup_airflow_config.sh --external --verify
```

### Erreur de connexion à PostgreSQL

```bash
# Vérifier que le container postgres-data est démarré
docker-compose ps postgres-data

# Tester la connexion depuis l'hôte
psql -h localhost -p 5433 -U datauser -d business_data

# Vérifier les logs
docker-compose logs postgres-data
```

### Le DAG n'apparaît pas

```bash
# Vérifier les erreurs de parsing
docker-compose exec airflow-scheduler airflow dags list-import-errors

# Forcer le refresh des DAGs
docker-compose restart airflow-dag-processor

# Vérifier les logs
docker-compose logs -f airflow-dag-processor
```

## 🔒 Sécurité

### En Production

1. **Changez tous les mots de passe par défaut**:
   - Airflow web UI
   - PostgreSQL databases
   - API credentials

2. **Utilisez des secrets Docker**:
   ```yaml
   secrets:
     postgres_password:
       file: ./secrets/postgres_password.txt
   ```

3. **Activez HTTPS** sur l'interface Web

4. **Restreignez l'accès réseau** avec des règles firewall

5. **Utilisez un coffre-fort** pour les secrets (Vault, AWS Secrets Manager, etc.)

### Fichiers Sensibles

Ne commitez JAMAIS dans Git:
- `.env`
- `config/airflow_connections.json` (avec vraies credentials)
- Fichiers dans `config/exports/`

Ajoutez à `.gitignore`:
```
.env
config/airflow_connections.json
config/exports/
logs/
*.pyc
__pycache__/
```

## 📊 Monitoring

### Logs Importants

```bash
# Scheduler (orchestration)
docker-compose logs -f airflow-scheduler

# Worker/Executor (exécution des tâches)
docker-compose logs -f airflow-apiserver

# Base de données
docker-compose logs postgres postgres-data
```

### Métriques

L'interface Web fournit:
- Durée d'exécution des DAGs
- Taux de succès/échec
- Utilisation des ressources
- Historique des exécutions

Accédez à: http://localhost:8080/home

## 🆘 Support

### Logs de Debug

Pour activer les logs debug:

```bash
# Dans docker-compose.yml, ajouter:
AIRFLOW__LOGGING__LOGGING_LEVEL: DEBUG

# Redémarrer
docker-compose restart
```

### Ressources

- [Documentation Airflow](https://airflow.apache.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 📝 Checklist de Déploiement

- [ ] Prérequis installés (Docker, jq)
- [ ] Fichiers de configuration créés et validés
- [ ] Credentials API configurés
- [ ] Containers démarrés sans erreur
- [ ] Variables Airflow configurées
- [ ] Connexions Airflow testées
- [ ] DAG visible dans l'interface
- [ ] Test d'exécution du DAG réussi
- [ ] Notifications email configurées
- [ ] Monitoring et alertes en place
- [ ] Documentation équipe à jour