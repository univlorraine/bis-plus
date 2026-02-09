# Projet AMUE Import - Présentation Technique

## Vue d'ensemble

Le DAG `amue_multi_table_import` automatise l'import quotidien de données financières depuis l'API AMUE vers PostgreSQL.

| Paramètre | Valeur |
|-----------|--------|
| **Schedule** | `0 2 * * *` (tous les jours à 2h00) |
| **Max active runs** | 1 |
| **Retries** | 0 (gestion manuelle des erreurs) |

---

## Architecture du Workflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1 : PRÉPARATION                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────┐                                                   │
│   │  check_history  │  Vérifie les N derniers jours                     │
│   └────────┬────────┘                                                   │
│            │                                                            │
├────────────┼────────────────────────────────────────────────────────────┤
│            │              PHASE 2 : ATTENTE API                         │
├────────────┼────────────────────────────────────────────────────────────┤
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │  wait_for_api   │  Polling jusqu'à disponibilité                    │
│   └────────┬────────┘                                                   │
│            │                                                            │
├────────────┼────────────────────────────────────────────────────────────┤
│            │         PHASE 3 : SÉLECTION & VÉRIFICATION                 │
├────────────┼────────────────────────────────────────────────────────────┤
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │  select_tables  │  Filtre selon statut + historique                 │
│   └────────┬────────┘                                                   │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ verify_table ×N │  Vérifie statut + structure (parallèle)           │
│   └────────┬────────┘                                                   │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ validate_tables │  Arrête si erreur détectée                        │
│   └────────┬────────┘                                                   │
│            │                                                            │
├────────────┼────────────────────────────────────────────────────────────┤
│            │                  PHASE 4 : IMPORT                          │
├────────────┼────────────────────────────────────────────────────────────┤
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ prepare_table×N │  Crée table si dev, vérifie si prod               │
│   └────────┬────────┘                                                   │
│            │                                                            │
│            ▼                                                            │
│   ┌─────────────────┐                                                   │
│   │ import_data ×N  │  INSERT ou UPSERT avec pagination                 │
│   └────────┬────────┘                                                   │
│            │                                                            │
├────────────┼────────────────────────────────────────────────────────────┤
│            │               PHASE 5 : FINALISATION                       │
├────────────┼────────────────────────────────────────────────────────────┤
│            │                                                            │
│      ┌─────┴─────┐                                                      │
│      ▼           ▼                                                      │
│ ┌──────────┐ ┌─────────────┐                                            │
│ │  save_   │ │ send_report │                                            │
│ │ metadata │→│             │                                            │
│ └──────────┘ └─────────────┘                                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Description Détaillée des Tâches

### Phase 1 : Préparation

#### `check_history`

**Objectif** : Déterminer quelles données doivent être importées en analysant l'historique.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/services/status_checker.py` |
| **Classe** | `AMUEStatusChecker.check_historical_status()` |
| **Configuration** | `amue_max_history_days` (défaut: 7) |

**Fonctionnement** :
1. Récupère la date du dernier import réussi (`amue_last_successful_run`)
2. Vérifie le statut de chaque jour depuis cette date
3. Identifie les tables avec statut OK vs KO par jour

**Intérêt** :
- Permet de rattraper les imports manqués (weekends, pannes, tables delta KO lors d'imports précédents)
- Détecte les anomalies historiques avant import
- Évite de reimporter des données déjà à jour

**Sortie** :
```python
{
    'status_by_date': {
        '20240115': {'tables_status': {...}, 'nbtables_ko': 0},
        '20240114': {'tables_status': {...}, 'nbtables_ko': 1}
    },
    'dates_checked': ['2024-01-15', '2024-01-14']
}
```

---

### Phase 2 : Attente API

#### `wait_for_api`

**Objectif** : Attendre que l'API AMUE soit prête avant de continuer.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/services/polling_service.py` |
| **Classe** | `AMUEPollingService.wait_for_ready()` |
| **Timeout** | `amue_max_wait_hours` (défaut: 6h) |
| **Intervalle** | `amue_polling_interval_minutes` (défaut: 10min) |

**Fonctionnement** :
1. Appelle l'endpoint statut de l'API
2. Vérifie le code HTTP (doit être 200)
3. Vérifie que la variable `finish` est renseignée dans le JSON
4. Si pas prêt : attend et réessaie
5. Supporte le backoff exponentiel (optionnel)

**Intérêt** :
- L'API AMUE génère les données chaque nuit (traitement batch côté AMUE)
- Le DAG tourne à 2h mais les données peuvent ne pas être prêtes
- Évite les erreurs dues à des données incomplètes
- Timeout configurable pour éviter l'attente infinie

**Critères de succès** :
- Code HTTP 200 **ET** variable `finish` renseignée (date/heure de fin du batch AMUE)

---

### Phase 3 : Sélection & Vérification

#### `select_tables`

**Objectif** : Filtrer les tables configurées pour ne garder que celles à importer.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/operators/table_filter.py` |
| **Classe** | `AMUETableFilter.filter_tables()` |
| **Configuration** | `amue_tables_to_import` (JSON) |

**Fonctionnement** :
1. Charge la liste des tables configurées
2. Vérifie que chaque table existe dans le statut API
3. Vérifie que le statut est OK (pas en erreur côté AMUE)
4. Vérifie l'historique pour cette table
5. Détermine le type d'import : `full` ou `differential`

**Intérêt** :
- Centralise la configuration des tables à importer
- Détecte les tables manquantes **avant** de commencer l'import
- Envoie une notification immédiate si table absente
- Supporte l'import différentiel (delta) pour les performances

**Type d'import** :
- `full` : Toutes les données (première fois ou pas de delta configuré)
- `differential` : Uniquement les modifications depuis `last_import`

---

#### `verify_table` (×N en parallèle)

**Objectif** : Vérifier chaque table individuellement avant import.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/operators/table_verifier.py` |
| **Classe** | `AMUETableVerifier.verify_table()` |
| **Mapping** | `.expand(table_info=tables)` |

**Vérifications effectuées** :
1. **Statut** : La table a bien statut=OK dans l'API
2. **Structure** : Récupère les colonnes et types depuis l'API
3. **Clés primaires** : Utilise les PK configurées dans les variables Airflow (ou les obtient via API si absentes)
4. **Fingerprint** : Calcule le hash MD5 de la structure
5. **Changement** : Compare avec le fingerprint stocké

**Intérêt** :
- Exécution parallèle pour gagner du temps
- Détection des changements de structure côté AMUE
- Récupération automatique des clés primaires
- Protection contre les imports sur structures modifiées

**Fingerprint** :
- Hash MD5 de : colonnes + types + clés primaires
- Permet de détecter : ajout/suppression de colonne, changement de type, modification des PK

---

#### `validate_tables`

**Objectif** : Point de contrôle - arrêter le DAG si une vérification a échoué.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `dags/dag_amue_dynamic_table.py:171-202` |
| **Comportement** | Raise `AirflowException` si erreur |

**Fonctionnement** :
1. Agrège les résultats de toutes les vérifications
2. Si au moins une erreur : log détaillé + exception
3. Si tout OK : retourne la liste des tables validées

**Intérêt** :
- Point de décision unique avant l'import
- Évite les imports partiels (tout ou rien)
- Log centralisé des erreurs avec phase et table concernée

---

### Phase 4 : Import

#### `prepare_table` (×N en parallèle)

**Objectif** : Préparer la structure PostgreSQL pour recevoir les données.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/operators/table_manager.py` |
| **Classe** | `AMUETableManager.manage_table()` |

**Comportement selon l'environnement** :

| Environnement | Table existe | Action |
|---------------|--------------|--------|
| `dev` | Non | Création automatique |
| `dev` | Oui | Utilisation existante |
| `production` | Non | **ERREUR - Arrêt** |
| `production` | Oui | Utilisation existante |

**Intérêt** :
- Sécurité : pas de création en production
- Flexibilité : création automatique en dev pour faciliter les tests
- Génération DDL automatique depuis la structure API

---

#### `import_data` (×N en parallèle)

**Objectif** : Importer les données depuis l'API vers PostgreSQL.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/operators/data_importer.py` |
| **Classe** | `AMUEDataImporter.import_table()` |
| **Batch size** | `amue_import_batch_size` (défaut: 5000) |

**Fonctionnement** :
1. **Streaming** : Les données sont récupérées page par page (générateur)
2. **Pagination** : Paramètre `skip` pour parcourir toutes les données
3. **Batch insert** : Insertion par lots de 5000 lignes
4. **UPSERT** : Si import différentiel + clés primaires définies

**Types de requêtes** :
```sql
-- Import full (INSERT simple)
INSERT INTO table (col1, col2) VALUES (...), (...), ...

-- Import différentiel (UPSERT)
INSERT INTO table (col1, col2) VALUES (...)
ON CONFLICT (pk1, pk2) DO UPDATE SET col1=EXCLUDED.col1, ...
```

**Intérêt** :
- Streaming pour économiser la mémoire (pas de chargement complet)
- Batch pour optimiser les performances
- UPSERT pour éviter les doublons et mettre à jour les modifications
- Retry automatique en cas d'erreur réseau
- Rollback automatique si échec d'un batch

---

### Phase 5 : Finalisation

#### `save_metadata`

**Objectif** : Enregistrer les métadonnées pour les prochains imports.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/services/metadata_manager.py` |
| **Classe** | `AMUEMetadataManager.update_metadata()` |

**Données sauvegardées** :
- `last_import` : Date/heure du dernier import réussi
- `finger_print` : Hash de la structure importée

**Intérêt** :
- Permet l'import différentiel au prochain run
- Traçabilité des imports
- Détection des changements de structure

---

#### `send_report`

**Objectif** : Générer et envoyer le rapport d'exécution par email.

| Aspect | Détail |
|--------|--------|
| **Fichier** | `plugins/amue/notifications/report_generator.py` |
| **Classe** | `AMUEReportGenerator.generate_and_send()` |
| **Destinataires** | `amue_report_recipients` |

**Contenu du rapport** :
- Date et durée d'exécution
- Nombre de tables importées
- Nombre total de lignes
- Détail par table (lignes, type d'import)
- Temps d'attente polling

**Intérêt** :
- Notification automatique de succès
- Traçabilité pour l'équipe
- Détection rapide des anomalies (moins de lignes que d'habitude)

---

## Démonstrations des Cas d'Échec

Un script interactif est disponible pour démontrer les cas d'erreur :

```bash
./scripts/dev/demo_failures.sh
```

### Menu du Script

```
═══════════════════════════════════════════════════════════════
  Démonstrations des Cas d'Échec AMUE
═══════════════════════════════════════════════════════════════

Sélectionnez une démonstration :

  1) Table absente de l'API
  2) API indisponible (timeout simulé)
  3) Afficher les logs en temps réel
  4) Ouvrir MailHog (emails)
  5) Ouvrir Airflow UI
  6) Restaurer configuration normale

  0) Quitter
```

---

### Demo 1 : Table Absente de l'API

**Commande** : `./scripts/dev/demo_failures.sh 1`

**Ce que fait le script** :
1. Sauvegarde la configuration actuelle
2. Ajoute `TABLE_INEXISTANTE` à la liste des tables
3. Déclenche le DAG

**Comportement attendu** :
- La tâche `select_tables` détecte la table manquante
- Email d'erreur envoyé avec liste des tables absentes
- DAG en échec

**Point de blocage** : `plugins/amue/operators/table_filter.py:66-80`

```python
if missing_tables:
    self._send_missing_tables_notification(missing_tables, current_status)
    raise TableNotFoundError(
        missing_tables=missing_tables,
        configured_count=len(self.tables_config),
        found_count=len(current_status)
    )
```

---

### Demo 2 : API Indisponible (Timeout)

**Commande** : `./scripts/dev/demo_failures.sh 2`

**Ce que fait le script** :
1. Sauvegarde l'endpoint actuel
2. Modifie l'endpoint vers une URL invalide
3. Réduit le timeout à 3 minutes (0.05h) pour la démo
4. Déclenche le DAG

**Comportement attendu** :
- La tâche `wait_for_api` tente de contacter l'API
- Logs : `[POLLING] Tentative 1/3 - Code HTTP: erreur`
- Après 3 tentatives : timeout et échec

**Point de blocage** : `plugins/amue/services/polling_service.py:256-288`

```python
error_msg = (
    f"Timeout: API pas prête après {self.config.max_wait_hours}h "
    f"({attempt} tentatives, dernier code: {last_status_code})"
)
raise AirflowException(error_msg)
```

---

### Commandes de Support

```bash
# Voir les logs en temps réel
./scripts/dev/demo_failures.sh 3

# Ouvrir MailHog (emails)
./scripts/dev/demo_failures.sh 4
# Ou : http://localhost:8025

# Ouvrir Airflow UI
./scripts/dev/demo_failures.sh 5
# Ou : http://localhost:8080 (admin/admin)

# Restaurer la configuration normale
./scripts/dev/demo_failures.sh 6
```

---

## Script de Gestion : manage.sh

### Présentation

Le script `manage.sh` est le **point d'entrée unique** pour gérer l'ensemble du projet. Il centralise toutes les opérations courantes et évite de retenir des commandes Docker complexes.

```bash
./manage.sh [COMMANDE] [ARGUMENTS]
```

### Intérêts du Script

| Avantage | Description |
|----------|-------------|
| **Centralisation** | Une seule commande pour tout gérer |
| **Abstraction Docker** | Pas besoin de connaître les commandes docker-compose |
| **Compatibilité** | Détecte automatiquement `docker-compose` ou `docker compose` |
| **Feedback visuel** | Messages colorés (INFO, SUCCESS, WARNING, ERROR) |
| **Documentation intégrée** | `./manage.sh help` affiche toutes les options |

### Catégories de Commandes

#### Gestion des Services

| Commande | Description | Exemple |
|----------|-------------|---------|
| `start` | Démarre tous les conteneurs Docker | `./manage.sh start` |
| `stop` | Arrête tous les conteneurs | `./manage.sh stop` |
| `restart` | Redémarre tous les conteneurs | `./manage.sh restart` |
| `status` | Affiche l'état des services | `./manage.sh status` |
| `logs` | Affiche les logs (tous ou un service) | `./manage.sh logs airflow-scheduler` |

**Intérêt** : Gestion simplifiée du cycle de vie des 7 conteneurs Docker (Airflow, PostgreSQL, MailHog, etc.)

---

#### Configuration

| Commande | Description | Exemple |
|----------|-------------|---------|
| `setup` | Installation complète initiale | `./manage.sh setup` |
| `config` | Reconfigure variables et connexions | `./manage.sh config` |
| `fix` | Corrige la configuration (avec attente API) | `./manage.sh fix` |
| `auto-fix` | Correction automatique complète | `./manage.sh auto-fix` |
| `verify` | Vérifie la configuration actuelle | `./manage.sh verify` |
| `verify-email` | Vérifie le correctif email Airflow 3.x | `./manage.sh verify-email` |
| `export` | Exporte la configuration actuelle | `./manage.sh export` |
| `diagnose` | Diagnostic complet du système | `./manage.sh diagnose` |

**Intérêt** :
- Installation en une commande (`setup`)
- Diagnostic automatique des problèmes (`diagnose`)
- Correction sans intervention manuelle (`auto-fix`)

---

#### Airflow

| Commande | Description | Exemple |
|----------|-------------|---------|
| `dags` | Liste tous les DAGs | `./manage.sh dags` |
| `trigger` | Déclenche un DAG manuellement | `./manage.sh trigger amue_multi_table_import` |
| `pause` | Met en pause un DAG | `./manage.sh pause amue_multi_table_import` |
| `unpause` | Réactive un DAG | `./manage.sh unpause amue_multi_table_import` |
| `variables` | Liste les variables Airflow | `./manage.sh variables` |
| `connections` | Liste les connexions Airflow | `./manage.sh connections` |

**Intérêt** : Contrôle des DAGs sans passer par l'interface web

---

#### Base de Données

| Commande | Description | Exemple |
|----------|-------------|---------|
| `db-shell` | Connexion au shell PostgreSQL | `./manage.sh db-shell` |
| `db-backup` | Sauvegarde la base de données | `./manage.sh db-backup` |
| `db-restore` | Restaure une sauvegarde | `./manage.sh db-restore backups/file.sql` |

**Intérêt** :
- Accès direct à la base métier pour debug
- Sauvegardes horodatées automatiques
- Restauration sécurisée avec confirmation

---

#### Développement

| Commande | Description | Exemple |
|----------|-------------|---------|
| `test` | Test un DAG | `./manage.sh test amue_multi_table_import` |
| `test-email` | Test la configuration email | `./manage.sh test-email` |
| `shell` | Shell bash dans le container Airflow | `./manage.sh shell` |
| `python` | Console Python interactive | `./manage.sh python` |
| `clean` | Nettoie les fichiers temporaires | `./manage.sh clean` |
| `version` | Affiche les versions | `./manage.sh version` |

**Intérêt** :
- Test des DAGs sans déclencher de vraie exécution
- Accès au shell pour debug avancé
- Console Python avec contexte Airflow

---

### Exemples d'Utilisation

#### Démarrage Quotidien
```bash
# Démarrer l'environnement
./manage.sh start

# Vérifier que tout est OK
./manage.sh status
./manage.sh verify
```

#### Debugging
```bash
# Voir les logs du scheduler
./manage.sh logs airflow-scheduler -f

# Diagnostic complet
./manage.sh diagnose

# Accès shell pour investigation
./manage.sh shell
```

#### Opérations DAG
```bash
# Déclencher manuellement
./manage.sh trigger amue_multi_table_import

# Mettre en pause (maintenance)
./manage.sh pause amue_multi_table_import

# Réactiver
./manage.sh unpause amue_multi_table_import
```

#### Sauvegarde / Restauration
```bash
# Créer une sauvegarde avant modification
./manage.sh db-backup

# Restaurer si problème
./manage.sh db-restore backups/business_data_20240115_143000.sql
```

---

### Affichage du Script

```
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                   Gestionnaire Airflow AMUE                   ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

[INFO] Démarrage des services...
[SUCCESS] Services démarrés

[INFO] Interfaces disponibles:
  - Airflow UI : http://localhost:8080
  - MailHog UI : http://localhost:8025
```

---

## Points Positifs du Projet

| Point | Description | Bénéfice |
|-------|-------------|----------|
| **Architecture modulaire** | Séparation hooks/operators/services/notifications | Maintenance facilitée, code réutilisable |
| **Gestion erreurs robuste** | Notifications email, rollback, retry | Détection rapide des problèmes |
| **Import différentiel** | UPSERT basé sur delta temporel | Performance (30s vs 10min) |
| **Fingerprint structure** | Hash MD5 colonnes+types+PK | Détection auto des changements schéma |
| **Sécurité production** | Création tables interdite en prod | Pas de modification accidentelle |
| **Logging unifié** | Logger centralisé avec préfixes | Traçabilité, debug facilité |
| **Configuration flexible** | Variables Airflow pour tous paramètres | Pas de redéploiement pour modifier |
| **Credentials sécurisés** | Stockage `.env` uniquement | Pas de secrets dans le code |
| **Streaming données** | Générateur Python pour l'import | Mémoire constante même gros volumes |
| **Parallélisation** | Tasks verify/prepare/import en parallèle | Temps d'exécution réduit |
| **Tests automatisés** | 449 tests unitaires (pytest) | Détection rapide des régressions |

---

## Points Négatifs / Axes d'Amélioration

| Point | Impact | Effort | Recommandation |
|-------|--------|--------|----------------|
| **Pas de monitoring** | Détection tardive problèmes | Moyen | Prometheus + Grafana |
| **Pas de dry-run** | Impossible tester sans impacter | Faible | Mode simulation |
| **Config dispersée** | Variables Airflow + JSON + .env | Faible | Fichier unique par env |
| **Documentation dense** | Difficile d'accès rapide | Faible | Quickstart séparé |
| **Pas de rotation secrets** | Sécurité limitée | Moyen | Intégrer Vault |
| **Logs volumineux** | Difficile à analyser | Faible | Niveaux de log configurables |
| **Pas d'alerting proactif** | Réaction uniquement sur échec | Moyen | Alertes sur anomalies (moins de lignes) |

---

## Variables Airflow

### Configuration Générale

| Variable | Description | Défaut |
|----------|-------------|--------|
| `environment` | `dev` ou `production` | `production` |
| `universite` | Code université pour l'API | - |

### Configuration API

| Variable | Description | Défaut |
|----------|-------------|--------|
| `api_endpoint_admin` | Endpoint statut/admin | - |
| `api_endpoint_table` | Endpoint données tables | - |
| `amue_api_max_retries` | Nombre de retry API | 3 |
| `amue_api_retry_delay_seconds` | Délai entre retries | 30 |

### Configuration Polling

| Variable | Description | Défaut |
|----------|-------------|--------|
| `amue_polling_interval_minutes` | Intervalle entre tentatives | 10 |
| `amue_max_wait_hours` | Timeout total polling | 6 |
| `amue_polling_exponential_backoff` | Activer backoff exponentiel | false |

### Configuration Import

| Variable | Description | Défaut |
|----------|-------------|--------|
| `amue_tables_to_import` | Configuration JSON des tables | - |
| `amue_max_history_days` | Jours d'historique à vérifier | 7 |
| `amue_import_batch_size` | Taille batch insertion | 5000 |

### Configuration Notifications

| Variable | Description | Défaut |
|----------|-------------|--------|
| `amue_report_recipients` | Destinataires emails (CSV) | - |
| `smtp_host` | Serveur SMTP | mailhog |
| `smtp_port` | Port SMTP | 1025 |

---

## Structure du Code

### Arborescence Complète

```
DemoDAGS/
│
├── manage.sh                     # Point d'entrée principal (gestion projet)
├── docker-compose.yml            # Configuration des 7 conteneurs
├── .env                          # Credentials (non versionné)
├── .env.example                  # Template des variables d'environnement
│
├── dags/
│   └── dag_amue_dynamic_table.py # DAG principal d'import
│
├── plugins/amue/                 # Code métier
│   │
│   ├── hooks/
│   │   └── amue_api_hook.py      # OAuth2 + appels API REST
│   │
│   ├── operators/
│   │   ├── data_importer.py      # Import streaming + batch
│   │   ├── table_filter.py       # Sélection tables + détection absentes
│   │   ├── table_manager.py      # DDL PostgreSQL (create/check)
│   │   └── table_verifier.py     # Vérification statut/structure/fingerprint
│   │
│   ├── services/
│   │   ├── metadata_manager.py   # Sauvegarde last_import + fingerprint
│   │   ├── polling_service.py    # Attente API avec retry/backoff
│   │   └── status_checker.py     # Vérification historique + statut courant
│   │
│   ├── notifications/
│   │   ├── email_service.py      # Service SMTP générique
│   │   ├── notification_service.py # Callback erreur Airflow
│   │   ├── report_generator.py   # Génération rapport succès
│   │   ├── notifiers/            # Notifiers spécialisés
│   │   └── templates/            # Templates HTML emails
│   │
│   └── utils/
│       ├── airflow_helpers.py    # Gestion variables Airflow
│       ├── logger.py             # Logger centralisé
│       ├── settings.py           # Configuration
│       └── transformers.py       # Types SQLite→PG + fingerprint
│
├── scripts/
│   ├── install/
│   │   ├── quick_setup.sh        # Installation complète
│   │   ├── setup_airflow_config.sh # Configuration Airflow
│   │   └── ensure_mailhog.sh     # Vérification MailHog
│   │
│   ├── manage/
│   │   ├── fix_config.sh         # Correction configuration
│   │   ├── auto_fix.sh           # Correction automatique
│   │   └── diagnose.sh           # Diagnostic système
│   │
│   └── dev/
│       ├── demo_failures.sh      # Démonstrations échecs
│       ├── test_email.sh         # Test envoi email
│       └── verify_email_fix.sh   # Vérification correctif email
│
├── config/
│   ├── airflow_connections.json  # Connexions (sans credentials)
│   └── airflow_variables.json    # Variables par défaut
│
└── Documentation/                # Documentation détaillée
```

### Description des Répertoires

| Répertoire | Rôle |
|------------|------|
| `dags/` | Définition du DAG Airflow |
| `plugins/amue/` | Code métier modulaire |
| `scripts/install/` | Scripts d'installation |
| `scripts/manage/` | Scripts de maintenance |
| `scripts/dev/` | Outils de développement et démo |
| `config/` | Fichiers de configuration JSON |
| `Documentation/` | Documentation technique |

---

## Récapitulatif des Commandes

### Commandes Essentielles

```bash
# Gestion du projet
./manage.sh start              # Démarre l'environnement
./manage.sh stop               # Arrête l'environnement
./manage.sh status             # État des services
./manage.sh diagnose           # Diagnostic complet

# DAG
./manage.sh trigger amue_multi_table_import   # Déclenchement manuel
./manage.sh logs airflow-scheduler -f         # Logs en temps réel

# Base de données
./manage.sh db-shell           # Console PostgreSQL
./manage.sh db-backup          # Sauvegarde

# Démos
./scripts/dev/demo_failures.sh # Démonstrations interactives
```

### Interfaces Web

| Interface | URL | Identifiants |
|-----------|-----|--------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MailHog | http://localhost:8025 | - |

### Aide

```bash
./manage.sh help               # Affiche toutes les commandes disponibles
```
