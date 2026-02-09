# Guide Opérationnel DemoDAGS

Ce document décrit les procédures opérationnelles quotidiennes pour le système DemoDAGS.

## Table des matières

1. [Démarrage et arrêt](#démarrage-et-arrêt)
2. [Monitoring quotidien](#monitoring-quotidien)
3. [Gestion des DAGs](#gestion-des-dags)
4. [Procédures de maintenance](#procédures-de-maintenance)
5. [Gestion Blue/Green](#gestion-bluegreen)
6. [Contacts et escalade](#contacts-et-escalade)

---

## Démarrage et arrêt

### Démarrer les services

```bash
# Démarrer le scheduler (en arrière-plan)
airflow scheduler &

# Démarrer le webserver (en arrière-plan)
airflow webserver -p 8080 &

# Ou avec systemd
sudo systemctl start airflow-scheduler
sudo systemctl start airflow-webserver
```

### Arrêter les services

```bash
# Arrêt propre
airflow scheduler stop
airflow webserver stop

# Ou avec systemd
sudo systemctl stop airflow-scheduler
sudo systemctl stop airflow-webserver
```

### Vérifier le statut

```bash
# Vérifier les processus
ps aux | grep airflow

# Avec systemd
sudo systemctl status airflow-scheduler
sudo systemctl status airflow-webserver
```

---

## Monitoring quotidien

### Vérifications de routine

1. **Interface Airflow** : Accéder à `http://localhost:8080`
2. **Vérifier les DAG runs** :
   - `dag_amue_dynamic_table` : Import principal
   - Statut : Vert = succès, Rouge = échec, Jaune = en cours

### Indicateurs clés

| Métrique | Seuil normal | Action si dépassé |
|----------|--------------|-------------------|
| Durée import | < 2h | Vérifier API/DB |
| Erreurs 24h | 0 | Analyser les logs |
| Tables importées | 32+ | Vérifier configuration |

### Consulter les logs

```bash
# Logs du scheduler
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log

# Logs d'un DAG spécifique
tail -f $AIRFLOW_HOME/logs/dag_id=dag_amue_dynamic_table/run_id=*/task_id=*/*.log

# Logs avec filtrage
grep -r "ERROR" $AIRFLOW_HOME/logs/dag_id=dag_amue_dynamic_table/
```

### Métriques de base de données

```sql
-- Nombre de lignes par table
SELECT table_name,
       (SELECT COUNT(*) FROM splus."table_name") as row_count
FROM information_schema.tables
WHERE table_schema = 'splus';

-- Dernier import par table
SELECT table_name, MAX(_imported_at) as last_import
FROM splus.csks  -- répéter pour chaque table
GROUP BY table_name;
```

---

## Gestion des DAGs

### Déclencher un import manuel

```bash
# Via CLI
airflow dags trigger dag_amue_dynamic_table

# Avec paramètres
airflow dags trigger dag_amue_dynamic_table --conf '{"table": "CSKS"}'
```

### Mettre en pause/reprendre un DAG

```bash
# Pause
airflow dags pause dag_amue_dynamic_table

# Reprendre
airflow dags unpause dag_amue_dynamic_table
```

### Effacer un DAG run échoué

```bash
# Effacer pour permettre une nouvelle exécution
airflow tasks clear dag_amue_dynamic_table -s 2024-01-01 -e 2024-01-02
```

### Vérifier l'état d'un DAG

```bash
# Liste des derniers runs
airflow dags list-runs -d dag_amue_dynamic_table --limit 10

# État des tâches d'un run
airflow tasks states-for-dag-run dag_amue_dynamic_table <run_id>
```

---

## Procédures de maintenance

### Maintenance hebdomadaire

1. **Nettoyer les anciens logs** :
   ```bash
   find $AIRFLOW_HOME/logs -type f -mtime +30 -delete
   ```

2. **Vérifier l'espace disque** :
   ```bash
   df -h
   du -sh $AIRFLOW_HOME/logs
   ```

3. **Vérifier la santé PostgreSQL** :
   ```sql
   -- Vérifier les connexions actives
   SELECT count(*) FROM pg_stat_activity;

   -- Vérifier les verrous
   SELECT * FROM pg_locks WHERE NOT granted;
   ```

### Maintenance mensuelle

1. **Vacuum des tables** :
   ```sql
   VACUUM ANALYZE splus_blue.csks;
   VACUUM ANALYZE splus_green.csks;
   -- Répéter pour toutes les tables
   ```

2. **Vérifier les index** :
   ```sql
   SELECT schemaname, tablename, indexname, idx_scan
   FROM pg_stat_user_indexes
   WHERE schemaname IN ('splus_blue', 'splus_green')
   ORDER BY idx_scan;
   ```

3. **Sauvegarder la configuration** :
   ```bash
   airflow variables export backup_$(date +%Y%m%d).json
   ```

### Nettoyer les métadonnées Airflow

```bash
# Supprimer les anciens DAG runs (> 90 jours)
airflow db clean --clean-before-timestamp $(date -d "90 days ago" +%Y-%m-%d) -y
```

---

## Gestion Blue/Green

### Vérifier l'état Blue/Green

```bash
# Via variable Airflow
airflow variables get amue_bluegreen_state
```

Exemple de sortie :
```json
{
  "active_schema": "blue",
  "inactive_schema": "green",
  "import_in_progress": false,
  "rollback_available": true
}
```

### Identifier le schéma actif

```sql
-- Vérifier vers où pointent les vues
SELECT table_name, view_definition
FROM information_schema.views
WHERE table_schema = 'splus'
LIMIT 1;
```

### Effectuer un rollback

1. **Vérifier la disponibilité** :
   ```bash
   airflow variables get amue_bluegreen_state | grep rollback_available
   ```

2. **Déclencher le rollback** :
   ```bash
   airflow dags trigger dag_amue_rollback
   ```

3. **Vérifier le résultat** :
   - Les vues dans `splus` pointent vers l'ancien schéma
   - L'état `active_schema` est inversé

### Forcer la libération d'un verrou

Si un import est bloqué (verrou abandonné) :

```bash
# Vérifier le verrou
airflow variables get amue_bluegreen_state | grep import_in_progress

# Réinitialiser l'état (ATTENTION: vérifier qu'aucun import n'est réellement en cours)
python -c "
from amue.services.bluegreen_manager import BlueGreenManager
manager = BlueGreenManager()
manager._force_release_lock()
print('Verrou libéré')
"
```

---

## Gestion des erreurs

### Erreur d'import d'une table

1. **Identifier la table en erreur** dans les logs
2. **Vérifier l'API** :
   ```bash
   curl -H "Authorization: Bearer $TOKEN" "https://api.amue.fr/.../TABLE_NAME"
   ```
3. **Réexécuter uniquement la table** :
   ```bash
   airflow tasks run dag_amue_dynamic_table import_TABLE_NAME <run_id>
   ```

### Erreur de connexion DB

1. **Vérifier PostgreSQL** :
   ```bash
   pg_isready -h localhost -p 5432
   ```
2. **Vérifier les connexions** :
   ```sql
   SELECT count(*) FROM pg_stat_activity WHERE state = 'active';
   ```
3. **Redémarrer si nécessaire** :
   ```bash
   sudo systemctl restart postgresql
   ```

### Erreur d'authentification API

1. **Vérifier la connexion Airflow** :
   ```bash
   airflow connections get amue_api
   ```
2. **Tester le token** :
   ```bash
   curl -X POST "https://oauth.amue.fr/token" \
     -d "client_id=..." -d "client_secret=..."
   ```

---

## Contacts et escalade

### Niveaux d'escalade

| Niveau | Délai | Contact |
|--------|-------|---------|
| 1 | 15 min | Équipe ops |
| 2 | 1h | Admin système |
| 3 | 4h | Développeur |

### Procédure d'escalade

1. **Documenter le problème** :
   - Heure de détection
   - Messages d'erreur
   - Actions tentées

2. **Notifier** selon le niveau d'urgence

3. **Créer un ticket** avec toutes les informations

### Urgences

- **Import bloqué > 4h** : Niveau 2
- **Base de données inaccessible** : Niveau 2
- **Perte de données** : Niveau 3

---

## Checklist quotidienne

- [ ] Vérifier le statut du DAG `dag_amue_dynamic_table`
- [ ] Vérifier les erreurs dans les logs des dernières 24h
- [ ] Vérifier l'espace disque (> 20% libre)
- [ ] Vérifier le nombre de connexions PostgreSQL
- [ ] Vérifier l'état Blue/Green si activé
