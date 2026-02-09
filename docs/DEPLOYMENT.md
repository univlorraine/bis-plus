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
| Apache Airflow | 2.5 | 2.7+ |
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

1. Créer la base de données :

```sql
CREATE DATABASE sifac_import;
CREATE USER airflow WITH PASSWORD 'votre_mot_de_passe';
GRANT ALL PRIVILEGES ON DATABASE sifac_import TO airflow;
```

2. Exécuter le script d'initialisation :

```bash
psql -U airflow -d sifac_import -f scripts/sql/init_db.sql
```

Ce script crée :
- Le schéma `splus` (vues)
- Les schémas `splus_blue` et `splus_green` (tables blue/green)
- Les permissions nécessaires

### Étape 4 : Configurer Airflow

1. Initialiser la base Airflow :

```bash
airflow db init
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
# Connexion PostgreSQL
airflow connections add 'postgres_data' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-schema 'sifac_import' \
    --conn-login 'airflow' \
    --conn-password 'votre_mot_de_passe' \
    --conn-port '5432'

# Connexion API AMUE
airflow connections add 'amue_api' \
    --conn-type 'http' \
    --conn-host 'api.amue.fr' \
    --conn-extra '{"oauth_url": "...", "client_id": "...", "client_secret": "..."}'
```

### Étape 5 : Configurer les variables Airflow

Importer les variables depuis le fichier de configuration :

```bash
airflow variables import config/airflow_variables.json
```

Variables importantes à personnaliser :
- `universite` : Code de votre université
- `environment` : `dev` ou `production`
- `amue_bluegreen_enabled` : `true` pour activer blue/green

---

## Configuration

### Variables Airflow principales

| Variable | Description | Exemple |
|----------|-------------|---------|
| `universite` | Code université | `UNI01` |
| `environment` | Environnement | `production` |
| `api_endpoint_table` | Template URL API | `https://api.amue.fr/$univ/$table` |
| `amue_bluegreen_enabled` | Activer blue/green | `true` |
| `amue_tables_to_import` | Tables à importer (JSON) | Voir ci-dessous |

### Configuration des tables

Le fichier `config/airflow_variables.json` contient la liste des tables :

```json
{
  "amue_tables_to_import": [
    {
      "name": "CSKS",
      "primary_key": "bukrs,kostl",
      "enabled": true,
      "delta": "aedat"
    }
  ]
}
```

### Configuration Blue/Green

Pour activer l'architecture blue/green :

1. Définir `amue_bluegreen_enabled` à `true`
2. Exécuter le script de migration si données existantes :

```bash
psql -U airflow -d sifac_import -f scripts/sql/migrate_to_bluegreen.sql
```

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
airflow dags test dag_amue_dynamic_table 2024-01-01
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

### Procédure de mise à jour

1. **Arrêter le scheduler Airflow** :
   ```bash
   airflow scheduler stop
   ```

2. **Sauvegarder la configuration** :
   ```bash
   airflow variables export backup_variables.json
   ```

3. **Mettre à jour le code** :
   ```bash
   git pull origin master
   pip install -r requirements.txt
   ```

4. **Appliquer les migrations SQL si nécessaire** :
   ```bash
   psql -U airflow -d sifac_import -f scripts/sql/migrations/XXXX.sql
   ```

5. **Redémarrer Airflow** :
   ```bash
   airflow scheduler &
   airflow webserver &
   ```

### Rollback

En cas de problème, utiliser le DAG `amue_rollback` si blue/green est activé :

1. Aller dans Airflow UI
2. Déclencher le DAG `amue_rollback`
3. Les vues basculeront vers le schéma précédent

---

## Troubleshooting

Voir [TROUBLESHOOTING.md](TROUBLESHOOTING.md) pour les problèmes courants.

## Support

Pour toute question :
- Consulter la documentation dans `docs/`
- Vérifier les logs Airflow
- Contacter l'équipe de support
