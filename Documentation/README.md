# Installation et Configuration d'Airflow 3 avec DAGs

## 📋 Prérequis

- Docker et Docker Compose installés
- Au moins 4 GB de RAM disponible
- Ports 8080, 5432, 5433 disponibles

## 🚀 Installation

### 1. Préparation de l'environnement

```bash
# Créer la structure des dossiers
mkdir -p airflow-demo/{dags,logs,plugins,config}
cd airflow-demo

# Copier le docker-compose.yml dans ce dossier

# Configurer les permissions (Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Ou pour Windows PowerShell
# echo "AIRFLOW_UID=50000" | Out-File -FilePath .env -Encoding ASCII
```

### 2. Démarrage des services

```bash
# Initialiser la base de données et créer l'utilisateur admin
docker-compose up airflow-init

# Démarrer tous les services
docker-compose up -d

# Vérifier que tout fonctionne
docker-compose ps
```

### 3. Accès à l'interface Web

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

## ⚙️ Configuration des Connections

### Connection PostgreSQL pour les données métier

Dans l'interface Airflow (Admin > Connections), créer une nouvelle connection:

- **Conn Id**: `postgres_data`
- **Conn Type**: `Postgres`
- **Host**: `postgres-data`
- **Schema**: `business_data`
- **Login**: `datauser`
- **Password**: `datapass`
- **Port**: `5432`

### Connection OAuth API pour AMUE

Créer une connection pour l'API AMUE:

- **Conn Id**: `oauth_api`
- **Conn Type**: `HTTP`
- **Host**: `https://sandbox.api.amue.fr`
- **Login**: `votre_client_id`
- **Password**: `votre_client_secret`
- **Extra**: 
```json
{
  "token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token",
  "api_base_url": "https://sandbox.api.amue.fr"
}
```

**Note importante**: L'API AMUE utilise **Basic Authentication** pour l'obtention du token (client_id et client_secret dans le header Authorization: Basic), puis **Bearer token** pour les appels API.

## 🔧 Configuration des Variables

Dans l'interface Airflow (Admin > Variables), créer:

- **oauth_api_connection_id**: `oauth_api`
- **api_endpoint**: `finances/cdv/v1/preprod/ul/table` (pour le DAG oauth_api_to_postgres)
- **amue_table_name**: `CSKS` (nom de la table à importer - pour le DAG amue_dynamic_table_import)

**Note pour AMUE**: Le DAG `amue_dynamic_table_import` récupère automatiquement la structure puis les données de la table spécifiée dans `amue_table_name`.

## 📦 Déploiement des DAGs

```bash
# Copier les DAGs dans le dossier dags/
cp dag_parallelism_demo.py airflow-demo/dags/
cp dag_retry_demo.py airflow-demo/dags/
cp dag_oauth_to_postgres.py airflow-demo/dags/

# Les DAGs seront automatiquement détectés (peut prendre 30-60 secondes)
```

## 🎯 Description des DAGs (Syntaxe Airflow 3)

### 1. `parallelism_demo` - Démonstration du parallélisme

**Fonctionnalités Airflow 3:**
- ✨ **@dag decorator** : définition déclarative du DAG
- 🎯 **@task decorator** : TaskFlow API pour Python operators
- 🔄 **Dynamic Task Mapping** avec `.expand()` : génère N tâches en parallèle
- 📦 **@task_group** : organisation logique des tâches parallèles
- 🔗 **Passage de données automatique** : XCom implicite entre @task
- 📊 **Variables Airflow** : stockage des résultats

**Syntaxe moderne:** Utilise `schedule='@daily'` au lieu de `schedule_interval`

**Exécution:** Planifié quotidiennement (@daily)

### 2. `retry_and_recovery_demo` - Gestion des échecs

**Fonctionnalités Airflow 3:**
- 🏗️ **@setup et @teardown decorators** : gestion du cycle de vie du DAG
- 🔀 **@task.branch()** : branchement conditionnel avec TaskFlow
- 🔄 **Retry automatique** avec backoff exponentiel
- 📞 **Callbacks personnalisés** (on_failure, on_retry, on_success)
- ⚡ **Trigger Rules avancées** (ALL_DONE, ONE_FAILED, etc.)
- 🧹 **Nettoyage automatique** avec teardown

**Nouveauté:** Les setup/teardown tasks garantissent l'exécution du nettoyage même en cas d'échec

**Exécution:** Planifié toutes les heures (@hourly)

### 3. `oauth_api_to_postgres` - Pipeline de données complet

**Fonctionnalités Airflow 3:**
- 🎨 **@dag et @task decorators** : pipeline déclaratif
- 🔐 **Hook OAuth personnalisé** pour authentification
- 📦 **@task_group** : regroupement de validations parallèles
- 🔗 **Type hints** : `list[dict]`, `dict` pour la validation
- 💾 **PostgresHook** pour insertion optimisée
- ⚙️ **UPSERT** : gestion des conflits (ON CONFLICT)
- ✅ **Validation et statistiques** en parallèle

**Avantage TaskFlow:** Pas besoin de gérer XCom manuellement, les données circulent automatiquement

**Exécution:** Toutes les 6 heures

## 🆕 Nouveautés Airflow 3 utilisées

### TaskFlow API (@dag, @task)
- **Avant (Airflow 2):** `with DAG(...) as dag:` + `PythonOperator`
- **Maintenant (Airflow 3):** `@dag()` et `@task()` decorators
- **Avantage:** Code plus concis, passage de données automatique (XCom implicite)

### Setup/Teardown Tasks
```python
@setup
def setup_environment():
    # S'exécute avant le workflow
    
@teardown  
def teardown_environment():
    # S'exécute après, même en cas d'échec
```

### Dynamic Task Mapping amélioré
```python
@task
def process_item(item_id: int) -> dict:
    # Traitement
    
# Expansion automatique
process_item.expand(item_id=items)
```

### Task Groups avec TaskFlow
```python
@task_group(tooltip='Description')
def my_group(data):
    task1 = my_task(data)
    task2 = another_task(data)
    task1 >> task2
```

### Type Hints natifs
Airflow 3 supporte pleinement les type hints Python modernes:
```python
def transform_data(api_response: dict) -> list[dict]:
    # Au lieu de List[Dict] avec typing
```

## 🧪 Test des DAGs

### Test du DAG de parallélisme

```bash
# Via l'interface web: activer le DAG et cliquer sur "Trigger DAG"

# Ou via CLI:
docker-compose exec airflow-webserver airflow dags test parallelism_demo 2024-12-09
```

### Test du DAG de retry

```bash
docker-compose exec airflow-webserver airflow dags test retry_and_recovery_demo 2024-12-09
```

### Test du DAG OAuth/PostgreSQL

Avant d'exécuter ce DAG, assurez-vous que:
1. La connection `postgres_data` est configurée
2. La connection `oauth_api` est configurée
3. Les variables sont définies

```bash
# Tester le DAG complet
docker-compose exec airflow-webserver airflow dags test oauth_api_to_postgres 2024-12-09

# Ou tester une tâche spécifique
docker-compose exec airflow-webserver airflow tasks test oauth_api_to_postgres create_table 2024-12-09
```

## 📊 Vérification des données dans PostgreSQL

```bash
# Se connecter à la base de données
docker-compose exec postgres-data psql -U datauser -d business_data

# Vérifier les données insérées
SELECT * FROM api_imports ORDER BY imported_at DESC LIMIT 10;

# Voir les statistiques
SELECT category, COUNT(*), AVG(item_value) 
FROM api_imports 
GROUP BY category;
```

## 🔍 Monitoring et Logs

### Voir les logs d'un DAG

```bash
# Logs du scheduler
docker-compose logs -f airflow-scheduler

# Logs du webserver
docker-compose logs -f airflow-webserver

# Ou dans l'interface web: cliquer sur une tâche > Log
```

### Voir l'état des DAGs

```bash
docker-compose exec airflow-webserver airflow dags list

# État détaillé d'un DAG
docker-compose exec airflow-webserver airflow dags state parallelism_demo 2024-12-09
```

## 🛠️ Commandes utiles

```bash
# Redémarrer Airflow
docker-compose restart

# Arrêter Airflow
docker-compose down

# Arrêter et supprimer les volumes (⚠️ supprime les données)
docker-compose down -v

# Voir les Variables Airflow
docker-compose exec airflow-webserver airflow variables list

# Créer une Variable
docker-compose exec airflow-webserver airflow variables set my_var my_value

# Lister les Connections
docker-compose exec airflow-webserver airflow connections list
```

## 🔐 Sécurité en Production

Pour un environnement de production:

1. **Changer les mots de passe par défaut**
2. **Générer une clé Fernet sécurisée:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
3. **Utiliser des secrets management** (Vault, AWS Secrets Manager, etc.)
4. **Activer HTTPS** sur le webserver
5. **Configurer l'authentification** (LDAP, OAuth, etc.)

## 📚 Ressources

- [Documentation Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [PostgreSQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/)

## ❓ Troubleshooting

### Les DAGs n'apparaissent pas

- Vérifier les logs: `docker-compose logs airflow-scheduler`
- Vérifier la syntaxe Python des DAGs
- Attendre 30-60 secondes pour la détection automatique

### Erreur de connection PostgreSQL

- Vérifier que le service est démarré: `docker-compose ps postgres-data`
- Vérifier la configuration de la connection dans Airflow
- Tester depuis le conteneur: 
```bash
docker-compose exec airflow-webserver psql -h postgres-data -U datauser -d business_data
```

### Problème de permissions

```bash
# Linux/Mac
sudo chown -R $(id -u):$(id -g) ./dags ./logs ./plugins

# Ou redémarrer l'init
docker-compose down
docker-compose up airflow-init
docker-compose up -d
```