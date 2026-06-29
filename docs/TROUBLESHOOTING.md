# Guide de Dépannage DemoDAGS

Ce document répertorie les erreurs fréquentes et leurs solutions.

## Table des matières

1. [Erreurs d'import](#erreurs-dimport)
2. [Erreurs de structure et fingerprints](#erreurs-de-structure-et-fingerprints)
3. [Erreurs de connexion API](#erreurs-de-connexion-api)
4. [Purge avant import](#purge-avant-import)
5. [Erreurs de base de données](#erreurs-de-base-de-données)
6. [Erreurs Blue/Green](#erreurs-bluegreen)
7. [Erreurs Airflow](#erreurs-airflow)
8. [Logs à consulter](#logs-à-consulter)
9. [Diagnostic rapide](#diagnostic-rapide)
10. [Contacts support](#contacts-support)

---

## Erreurs d'import

### AMUEDataError: Doublons detectes dans les donnees API

**Symptôme** : L'import échoue avec un message sur les doublons.

**Cause** : L'API retourne des données avec des clés primaires dupliquées.

**Solution** :
1. Vérifier les données source dans l'API
2. Contacter l'équipe AMUE si le problème persiste
3. Temporairement, filtrer les doublons :
   ```python
   # Dans la configuration, ajouter un filtre
   ```

### AMUEDataError: Table X sans primary_key définie

**Symptôme** : Import impossible car pas de clé primaire.

**Cause** : La table n'a pas de `primary_key` configurée dans les variables.

**Solution** :
1. Éditer la table `splus_admin.amue_tables` dans PostgreSQL :
   ```sql
   UPDATE splus_admin.amue_tables
   SET primary_key = 'col1,col2'
   WHERE table_name = 'TABLE_NAME';
   ```

### Import très lent (> 2 heures)

**Symptôme** : L'import prend beaucoup plus de temps que d'habitude.

**Causes possibles** :
- API AMUE lente
- Connexion réseau instable
- Base de données surchargée

**Solutions** :
1. Vérifier la latence API :
   ```bash
   time curl -s "https://api.amue.fr/.../health"
   ```
2. Vérifier les index PostgreSQL :
   ```sql
   EXPLAIN ANALYZE SELECT * FROM splus_blue.csks WHERE bukrs = 'XXX';
   ```
3. Vérifier le nombre de connexions :
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   ```

---

---

## Erreurs de structure et fingerprints

### Table en statut `blocked` — Changement de structure détecté


**Symptôme** : Le DAG `amue_table_setup` (ou `amue_multi_table_import`) échoue avec un message de fingerprint mismatch. La table est passée en statut `blocked`.

**Cause** : L'API AMUE a modifié la structure de la table (nouvelle colonne, type changé, colonne supprimée). Le système refuse d'importer pour éviter une corruption silencieuse.

**Diagnostic** :
```sql
-- Voir les tables bloquées
SELECT table_name, setup_status, updated_at
FROM splus_admin.amue_tables
WHERE setup_status = 'blocked';

-- Voir toutes les tables et leur statut
SELECT table_name, enabled, setup_status, primary_key
FROM splus_admin.amue_tables
ORDER BY setup_status, table_name;
```

**Solutions** :

Option 1 — Accepter le nouveau schéma (cas le plus courant) :
```sql
-- Réinitialiser les fingerprints pour forcer le recalcul
UPDATE splus_admin.amue_tables
SET setup_status     = 'pending',
    fingerprint_api  = '',
    fingerprint_local = '',
    updated_at       = NOW()
WHERE table_name = 'NOM_TABLE';
```
Puis relancer `amue_table_setup` pour recalculer les fingerprints sur la nouvelle structure.

Option 2 — Désactiver temporairement la table (si la modification n'est pas encore validée) :
```sql
UPDATE splus_admin.amue_tables
SET enabled = false
WHERE table_name = 'NOM_TABLE';
```
L'import continuera sans cette table.

---

### Table en statut `pending` après setup

**Symptôme** : Le DAG `amue_table_setup` s'est exécuté mais certaines tables restent en `pending`.

**Causes** :
- La table n'existe pas encore sur l'API AMUE
- Erreur de connexion API lors du setup (voir logs du DAG)
- Table désactivée côté API mais activée localement

**Solutions** :
1. Vérifier que la table est accessible sur l'API :
   ```bash
   # Via le monitoring ou manuellement
   curl -H "Authorization: Bearer $TOKEN" "$API_BASE/table/NOM_TABLE?limit=1"
   ```
2. Consulter les logs du DAG `amue_table_setup` pour la task correspondante
3. Si la table n'existe pas côté API, la désactiver :
   ```sql
   UPDATE splus_admin.amue_tables SET enabled = false WHERE table_name = 'NOM_TABLE';
   ```

---

### Import bloqué : `check_setup_status` échoue

**Symptôme** : La task `check_setup_status` de `amue_multi_table_import` échoue avec une liste de tables non prêtes.

**Cause** : Une ou plusieurs tables enabled ne sont pas en statut `ready` (elles sont `pending` ou `blocked`).

**Solution** : Consulter les statuts et traiter chaque table non prête (voir ci-dessus), puis relancer l'import.

```sql
-- Vue rapide du problème
SELECT table_name, setup_status
FROM splus_admin.amue_tables
WHERE enabled = true AND setup_status != 'ready';
```

---

## Erreurs de connexion API

### AMUEAuthError: Token invalide ou expiré

**Symptôme** : Erreur 401 sur les appels API.

**Cause** : Les credentials OAuth sont invalides ou expirés.

**Solution** :
1. Vérifier la connexion Airflow :
   ```bash
   airflow connections get oauth_api
   ```
2. Tester manuellement :
   ```bash
   curl -X POST "https://sandbox.auth.amue.fr/auth/fer/oauth/token" \
     -d "grant_type=client_credentials" \
     -d "client_id=YOUR_ID" \
     -d "client_secret=YOUR_SECRET"
   ```
3. Mettre à jour les credentials si nécessaire :
   ```bash
   airflow connections delete oauth_api
   # Recréer avec les bons credentials (voir INSTALL.md, Étape 6)
   ```

### AMUENetworkError: Connection timeout

**Symptôme** : Timeout lors de la connexion à l'API.

**Causes** :
- API AMUE indisponible
- Problème de pare-feu
- DNS non résolu

**Solutions** :
1. Tester la connectivité :
   ```bash
   ping api.amue.fr
   curl -v https://api.amue.fr/health
   ```
2. Vérifier le pare-feu
3. Augmenter le timeout dans la configuration :
   ```json
   {"amue_api_timeout_seconds": "120"}
   ```

### AMUEAPIError: HTTP 429 Too Many Requests

**Symptôme** : Rate limiting activé par l'API.

**Solution** :
1. Réduire la fréquence des appels
2. Augmenter le délai entre les retries :
   ```bash
   airflow variables set amue_api_retry_delay_seconds 60
   ```
3. Contacter AMUE pour augmenter la limite

### AMUEAPIError: HTTP 500/502/503

**Symptôme** : Erreur serveur côté API.

**Solution** :
1. Attendre et réessayer (erreur temporaire)
2. Vérifier le statut de l'API AMUE
3. Contacter le support AMUE si persistant

---

---

## Purge avant import

### La table n'est pas purgée malgré la variable configurée

**Symptôme** : La tâche `import_data[<NOM>]` s'exécute sans purger la table — la ligne `[IMPORT] <table> : purgée avant import` est absente des logs.

**Causes et vérifications** :

1. **Faute de frappe ou casse différente** — le nom dans la variable doit correspondre exactement (sans sensibilité à la casse, mais sans caractères parasites) :

   ```bash
   ./manage.sh var-get amue_tables_to_purge
   # Attendu : '["BKPF"]' ou '["bkpf"]' — les guillemets JSON sont obligatoires
   ```

2. **JSON invalide** — une syntaxe incorrecte fait tomber le parsing silencieusement vers `[]` :

   ```bash
   # INCORRECT (manque les guillemets autour du nom de table)
   ./manage.sh var-set amue_tables_to_purge '[BKPF]'
   # CORRECT
   ./manage.sh var-set amue_tables_to_purge '["BKPF"]'
   ```

3. **Table désactivée** — une table non active (`enabled = false`) n'apparaît pas dans la liste d'import et ne peut donc pas être purgée :

   ```sql
   SELECT table_name, enabled FROM splus_admin.amue_tables WHERE table_name = 'BKPF';
   ```

4. **Variable lue avant la modification** — Airflow charge les variables au démarrage de la tâche. Si vous modifiez la variable pendant qu'un import tourne, la tâche déjà démarrée utilisera l'ancienne valeur. La valeur sera prise en compte au prochain run.

### Erreur à l'exécution du TRUNCATE

**Symptôme** : La tâche `import_data[<NOM>]` échoue avec une erreur PostgreSQL sur le TRUNCATE.

**Causes** :

- **Permissions insuffisantes** : l'utilisateur `datauser` doit avoir le droit `TRUNCATE` sur les tables `splus_blue.*` / `splus_green.*`. Vérifier :
  ```sql
  SELECT has_table_privilege('datauser', 'splus_blue.bkpf', 'TRUNCATE');
  ```
  Si `false`, accorder le droit :
  ```sql
  GRANT TRUNCATE ON ALL TABLES IN SCHEMA splus_blue TO datauser;
  GRANT TRUNCATE ON ALL TABLES IN SCHEMA splus_green TO datauser;
  ```
- **Table inexistante dans le schéma cible** : la table n'a pas encore été créée par `amue_table_setup`. Lancer `amue_table_setup` avant l'import.

---

## Erreurs de base de données

### AMUEDatabaseError: Connection refused

**Symptôme** : Impossible de se connecter à PostgreSQL.

**Solutions** :
1. Vérifier que PostgreSQL tourne :
   ```bash
   pg_isready -h localhost -p 5432
   sudo systemctl status postgresql
   ```
2. Vérifier les logs PostgreSQL :
   ```bash
   tail -f /var/log/postgresql/postgresql-15-main.log
   ```
3. Redémarrer PostgreSQL si nécessaire :
   ```bash
   sudo systemctl restart postgresql
   ```

### AMUEDatabaseError: Too many connections

**Symptôme** : Erreur "too many clients already".

**Solutions** :
1. Vérifier les connexions actives :
   ```sql
   SELECT count(*), state FROM pg_stat_activity GROUP BY state;
   ```
2. Terminer les connexions idle :
   ```sql
   SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state = 'idle'
     AND query_start < now() - interval '1 hour';
   ```
3. Augmenter `max_connections` dans `postgresql.conf`

### AMUEBatchError: Conflit de clé primaire

**Symptôme** : Erreur sur une insertion avec conflit.

**Cause** : Données avec même clé primaire déjà présentes.

**Note** : En mode UPSERT (par défaut), ce problème ne devrait pas se produire.
Si l'erreur persiste, vérifier :
1. La définition de la clé primaire dans la configuration
2. Les contraintes d'unicité sur la table

### Erreur: Schema does not exist

**Symptôme** : `schema "splus_blue" does not exist`

**Solution** :
1. Exécuter le script d'initialisation :
   ```bash
   psql -U airflow -d business_data -f scripts/sql/init_db.sql
   ```
2. Vérifier les schémas :
   ```sql
   SELECT schema_name FROM information_schema.schemata
   WHERE schema_name LIKE 'splus%';
   ```

---

## Erreurs Blue/Green

### ConcurrentImportError: Un import est déjà en cours


**Symptôme** : Nouvel import impossible car un import est détecté en cours.

**Causes** :
- Un import est réellement en cours
- Un verrou abandonné (import précédent crashé)

**Diagnostic** :
```sql
SELECT
    import_in_progress,
    import_started_at,
    AGE(NOW(), import_started_at) AS duree,
    last_successful_run
FROM splus_admin.amue_state
WHERE id = 1;
```

- Si `duree` < 2h → attendre (import probablement en cours)
- Si `duree` > 2h ou `import_started_at` est NULL → verrou abandonné

**Solutions** :

Option 1 — Libération programmatique (recommandée) :
```python
from common.services.bluegreen.bluegreen_manager import BlueGreenManager
manager = BlueGreenManager()
manager._force_release_lock()
print("Verrou libéré")
```

Option 2 — SQL direct (urgence uniquement, après avoir vérifié qu'aucun import ne tourne) :
```sql
UPDATE splus_admin.amue_state
SET import_in_progress = false, updated_at = NOW()
WHERE id = 1;
```

### RollbackNotAvailableError: Rollback non disponible

**Symptôme** : Tentative de rollback impossible.

**Causes** :
- Aucun import n'a été effectué
- Le schéma inactif a été modifié manuellement

**Solution** :
- Le rollback n'est disponible que jusqu'au prochain import
- Restaurer depuis une sauvegarde si nécessaire

### ViewSwitchError: Erreur lors du switch des vues


**Symptôme** : Les vues n'ont pas basculé correctement.

**Solutions** :
1. Vérifier les erreurs dans les logs
2. Vérifier l'intégrité des vues :
   ```sql
   SELECT table_name, view_definition
   FROM information_schema.views
   WHERE table_schema = 'splus';
   ```
3. Recréer les vues manuellement si nécessaire :
   ```sql
   CREATE OR REPLACE VIEW splus.csks AS
   SELECT * FROM splus_blue.csks;
   ```

### Schémas désynchronisés

**Symptôme** : Les schémas blue et green ont des structures différentes.

**Solution** :
1. Identifier les différences :
   ```sql
   -- Colonnes dans blue mais pas dans green
   SELECT column_name FROM information_schema.columns
   WHERE table_schema = 'splus_blue' AND table_name = 'csks'
   EXCEPT
   SELECT column_name FROM information_schema.columns
   WHERE table_schema = 'splus_green' AND table_name = 'csks';
   ```
2. Synchroniser manuellement les structures entre les deux schémas

---

## Erreurs Airflow

### DAG non visible dans l'interface


**Causes** :
- Erreur de syntaxe dans le fichier DAG
- DAG non dans le bon répertoire

**Solutions** :
1. Vérifier les erreurs de parsing :
   ```bash
   airflow dags list-import-errors
   ```
2. Valider le DAG :
   ```bash
   python dags/dag_amue_dynamic_table.py  # vérifie la syntaxe Python du fichier
   ```
3. Vérifier le chemin `dags_folder` dans `airflow.cfg`

### Tâche bloquée en "running"


**Symptôme** : Une tâche reste en running sans progresser.

**Solutions** :
1. Vérifier les logs de la tâche
2. Vérifier si le worker est actif
3. Forcer l'échec et réexécuter :
   ```bash
   airflow tasks clear <dag_id> -t <task_id>
   ```

### Erreur: Variable not found

**Symptôme** : `Variable 'universite' does not exist`

**Solution** :
1. Créer la variable :
   ```bash
   airflow variables set universite "VOTRE_CODE"
   ```
2. Ou importer toutes les variables :
   ```bash
   airflow variables import config/airflow_variables.json
   ```

---

## Logs à consulter

### Emplacement des logs

| Type | Emplacement |
|------|-------------|
| Scheduler | `$AIRFLOW_HOME/logs/scheduler/` |
| DAG runs | `$AIRFLOW_HOME/logs/dag_id=.../` |
| Workers | `$AIRFLOW_HOME/logs/worker/` |
| PostgreSQL | `/var/log/postgresql/` |

### Rechercher des erreurs

```bash
# Erreurs récentes dans tous les logs
grep -r "ERROR" $AIRFLOW_HOME/logs --include="*.log" | tail -50

# Erreurs pour un DAG spécifique
grep -r "ERROR" $AIRFLOW_HOME/logs/dag_id=amue_multi_table_import/

# Avec contexte
grep -B5 -A5 "AMUEError" $AIRFLOW_HOME/logs/dag_id=amue_multi_table_import/
```

### Niveaux de log

Modifier le niveau dans `airflow.cfg` :
```ini
[logging]
logging_level = DEBUG  # ou INFO, WARNING, ERROR
```

Ou par variable d'environnement :
```bash
export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

---

## Diagnostic rapide

### Checklist de dépannage

1. **Vérifier Airflow** :
   - [ ] Scheduler actif
   - [ ] Webserver actif
   - [ ] Pas d'erreurs de parsing DAG

2. **Vérifier PostgreSQL** :
   - [ ] Service actif
   - [ ] Connexions disponibles
   - [ ] Schémas présents

3. **Vérifier l'API AMUE** :
   - [ ] Endpoint accessible
   - [ ] Token valide
   - [ ] Pas de rate limiting

4. **Vérifier les variables** :
   - [ ] `universite` définie
   - [ ] `api_endpoint_table` définie
   - [ ] Tables activées dans `splus_admin.amue_tables`

### Commande de diagnostic global

```bash
# Script de diagnostic
echo "=== Airflow ==="
airflow version
airflow dags list-import-errors

echo "=== PostgreSQL ==="
pg_isready -h localhost

echo "=== Variables ==="
airflow variables list | wc -l

echo "=== Derniers runs ==="
airflow dags list-runs -d amue_multi_table_import --limit 5
```

---

## Contacts support

- Documentation : `docs/`
- Issues : Créer un ticket dans le système de suivi
- Urgences : Voir [OPERATIONS.md](OPERATIONS.md#contacts-et-escalade)
