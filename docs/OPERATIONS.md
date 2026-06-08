# Guide d'utilisation — SifacPlus / DemoDAGS

Ce guide explique **comment utiliser le projet au quotidien** : quand
l'utiliser, comment se repérer dans les interfaces, comment réaliser les
opérations courantes (ajouter une table, ajouter une vue custom, modifier
le planning, etc.), comment lire les logs et comment gérer les erreurs.

Il est organisé par **scénarios d'usage** : chaque section répond à une
question concrète du type "je veux faire X".

## Table des matières

- [1. À quoi sert ce projet](#1-à-quoi-sert-ce-projet)
- [2. Comment se repérer](#2-comment-se-repérer)
- [3. Scénarios courants](#3-scénarios-courants)
  - [3.1 Lancer un import manuel](#31-lancer-un-import-manuel)
  - [3.2 Ajouter une nouvelle table à importer](#32-ajouter-une-nouvelle-table-à-importer)
  - [3.3 Désactiver ou supprimer une table](#33-désactiver-ou-supprimer-une-table)
  - [3.4 Ajouter une vue SQL custom](#34-ajouter-une-vue-sql-custom)
  - [3.5 Modifier une vue custom existante](#35-modifier-une-vue-custom-existante)
  - [3.6 Modifier l'heure d'un import](#36-modifier-lheure-dun-import)
  - [3.7 Ajuster les performances de l'import](#37-ajuster-les-performances-de-limport)
  - [3.8 Changer les destinataires des rapports](#38-changer-les-destinataires-des-rapports)
  - [3.9 Tester un import sans attendre l'API](#39-tester-un-import-sans-attendre-lapi)
  - [3.10 Annuler un import (rollback)](#310-annuler-un-import-rollback)
  - [3.11 Chaîner des DAGs avant ou après l'import](#311-chaîner-des-dags-avant-ou-après-limport)
  - [3.12 Renouveler les credentials OAuth AMUE](#312-renouveler-les-credentials-oauth-amue)
  - [3.13 Consulter les emails en dev (MailHog)](#313-consulter-les-emails-en-dev-mailhog)
  - [3.14 Sauvegarder la base métier](#314-sauvegarder-la-base-métier)
  - [3.15 Importer une table depuis Oracle ECC](#315-importer-une-table-depuis-oracle-ecc)
  - [3.16 Ré-importer des tables spécifiques (correction)](#316-ré-importer-des-tables-spécifiques-correction)
  - [3.17 Reprendre un import après échec partiel](#317-reprendre-un-import-après-échec-partiel)
  - [3.18 Consulter l'historique des imports](#318-consulter-lhistorique-des-imports)
  - [3.19 Gérer les utilisateurs Airflow](#319-gérer-les-utilisateurs-airflow)
- [4. Comprendre les logs](#4-comprendre-les-logs)
- [5. Gérer les erreurs](#5-gérer-les-erreurs)
- [6. Superviser l'état du système](#6-superviser-létat-du-système)
- [7. Référence rapide](#7-référence-rapide)

---

## 1. À quoi sert ce projet

Le projet **importe quotidiennement les données financières AMUE (SIFAC+)**
dans une base PostgreSQL. Il peut aussi importer des tables depuis une base
**Oracle ECC**.

Ce que vous obtenez en sortie : un schéma `splus` qui expose des vues
lisibles par vos outils BI / vos applications métier. Les applications ne
voient que `splus.*` et ne savent pas sur quel schéma physique elles
pointent — c'est l'architecture **blue/green** qui permet des imports
atomiques et un rollback instantané.

Quand intervenir ?

- **Tous les jours** : vérifier que l'import de la nuit s'est bien passé
  ([6](#6-superviser-létat-du-système)).
- **Ponctuellement** : ajouter/retirer une table ([3.2](#32-ajouter-une-nouvelle-table-à-importer), [3.3](#33-désactiver-ou-supprimer-une-table)), ajouter une
  vue métier ([3.4](#34-ajouter-une-vue-sql-custom)), ajuster le planning ([3.6](#36-modifier-lheure-dun-import)), résoudre une erreur ([5](#5-gérer-les-erreurs)).
- **Jamais en routine** : toucher au code, modifier manuellement les
  tables blue/green. Le projet est conçu pour que **toute la configuration
  passe par des variables Airflow, le CLI `./manage.sh`, ou des fichiers
  SQL**.

---

## 2. Comment se repérer

### 2.1 Les points d'entrée

| Outil                 | Où ?                                          | À quoi ça sert                                                    |
|-----------------------|-----------------------------------------------|-------------------------------------------------------------------|
| **Airflow UI**        | http://localhost:8080 (`airflow` / `airflow`) | Déclencher les DAGs, voir l'historique, lire les logs à la souris |
| **CLI `./manage.sh`** | racine du projet (`./manage.sh help`)         | Tout ce qu'on peut faire à la ligne de commande                   |
| **MailHog**           | http://localhost:8025                         | Lire les emails de rapport en environnement dev                   |
| **PostgreSQL métier** | `./manage.sh db-shell` (ou `localhost:5433`)  | Requêter les données, voir l'état de la stack                     |

### 2.2 Les DAGs que vous allez croiser

| DAG                        | Rôle                                           | Quand on y touche                                          |
|----------------------------|------------------------------------------------|------------------------------------------------------------|
| `amue_multi_table_import`  | Import principal (quotidien)                   | Tous les jours (surveillance) ; relance manuelle si échec  |
| `amue_correction_import`   | Ré-import AMUE pour des tables spécifiques     | Correction ciblée sans attendre l'heure planifiée          |
| `amue_table_setup`         | Préparation des tables (structure, empreintes) | Après **toute** modification de la liste des tables        |
| `amue_refresh_views`       | Recrée les vues SQL custom                     | Après ajout/modification d'un fichier `custom_views/*.sql` |
| `amue_sync_schemas`        | Synchronise blue ↔ green                       | Après une intervention manuelle sur un schéma              |
| `amue_rollback`            | Repointe les vues vers l'état précédent        | En cas d'import foireux, avant le prochain import          |
| `amue_status_monitor`      | Fenêtre de surveillance de l'API AMUE          | Rarement — surveillance avancée                            |
| `ecc_multi_table_import`   | Import depuis Oracle ECC                       | Tous les jours si ECC est activé                           |
| `ecc_correction_import`    | Ré-import ECC pour des tables spécifiques      | Correction ciblée ECC sans attendre l'heure planifiée      |

### 2.3 Les schémas PostgreSQL

```
splus            ← vues publiques (ce que lisent les applis)
splus_blue       ← tables "Blue" (un des deux jeux de données)
splus_green      ← tables "Green" (l'autre jeu)
splus_admin      ← administration : amue_state, amue_tables
```

À un instant donné, **les vues `splus.*` pointent toutes vers le même
schéma** (blue *ou* green). Le basculement se fait à la fin d'un import
réussi, de façon atomique.

> Ne modifiez **jamais** directement les tables de `splus_blue` ou
> `splus_green` : vous casseriez l'invariant blue/green.

### 2.4 Les deux tables d'administration

- **`splus_admin.amue_state`** — singleton (`id = 1`) qui décrit l'état du
  système : quel schéma est actif, y a-t-il un import en cours, quand a eu
  lieu le dernier succès, etc.
- **`splus_admin.amue_tables`** — la liste des tables à importer. C'est
  **ici** que vous ajoutez/retirez des tables ([3.2](#32-ajouter-une-nouvelle-table-à-importer), [3.3](#33-désactiver-ou-supprimer-une-table)).

---

## 3. Scénarios courants

Chaque scénario suit le même format : **Objectif → Étapes → Vérifier**.

### 3.1 Lancer un import manuel

**Quand** : relance après un échec corrigé, rattrapage, test.

**Étapes** :

```bash
./manage.sh trigger amue_multi_table_import
```

Ou via l'UI : http://localhost:8080 → ligne `amue_multi_table_import` → bouton ▶.

**Vérifier** :

- UI Airflow → vue **Graph** ou **Grid** du run → toutes les tâches
  doivent finir en vert.
- Un email de rapport arrive dans MailHog (dev) ou dans la boîte
  configurée (prod).
- En SQL (`./manage.sh db-shell`) :

```sql
SELECT last_successful_run, active_schema
  FROM splus_admin.amue_state WHERE id = 1;
```

### 3.2 Ajouter une nouvelle table à importer

**Quand** : AMUE publie une nouvelle table, ou vous voulez traiter une
table déjà disponible mais absente de la configuration.

**Étapes** :

1. **Inscrire la table** dans `splus_admin.amue_tables`.

   Cas simple (clé primaire simple, import complet) :

   ```bash
   ./manage.sh add-table NOM_TABLE
   ```

   Cas complet (clé composite et/ou import différentiel) :

   ```bash
   ./manage.sh db-shell
   ```

   ```sql
   INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta)
   VALUES ('BKPF', true, 'BUKRS,BELNR,GJAHR', 'cpudt')
   ON CONFLICT (table_name) DO UPDATE
   SET enabled     = EXCLUDED.enabled,
       primary_key = EXCLUDED.primary_key,
       delta       = EXCLUDED.delta;
   ```

   > - `primary_key` : liste **séparée par des virgules**, sans espaces.
   >   Ces colonnes servent au UPSERT (elles priment sur ce que l'API déclare).
   > - `delta` : nom de la colonne de date pour un import incrémental
   >   (`cpudt`, `aedat`, …). Laissez **vide** (`''`) pour un import complet.

2. **Préparer la table** (création DDL + calcul des empreintes) :

   ```bash
   ./manage.sh trigger amue_table_setup
   ```

3. **Vérifier** :

   ```bash
   ./manage.sh list-tables
   ```

   La nouvelle table doit apparaître avec `setup_status = ready`. Si elle
   est `pending` ou `blocked`, voir [5.2](#52-une-table-reste-pending-ou-passe-blocked).

4. **Importer les données** :

   ```bash
   ./manage.sh trigger amue_multi_table_import
   ```

   Ou attendre l'import nocturne.

### 3.3 Désactiver ou supprimer une table

**Désactivation** (on garde l'historique, on n'importe plus) :

```bash
./manage.sh disable-table KNA1
```

**Réactivation** :

```bash
./manage.sh enable-table KNA1
```

**Suppression complète** (on retire la ligne de config — les tables
physiques dans `splus_blue`/`splus_green` restent, mais ne sont plus
alimentées ni basculées) :

```bash
./manage.sh remove-table KNA1
```

Si vous voulez aussi supprimer les **données** :

```bash
./manage.sh db-shell
```

```sql
DROP TABLE IF EXISTS splus_blue.kna1;
DROP TABLE IF EXISTS splus_green.kna1;
DROP VIEW  IF EXISTS splus.kna1;
```

### 3.4 Ajouter une vue SQL custom

**Quand** : vous voulez exposer une vue métier (jointure, agrégation) en
plus des vues "1 pour 1" sur les tables AMUE.

**Étapes** :

1. **Créer le fichier SQL** dans `scripts/sql/custom_views/`. Un fichier
   par vue, nommé comme la vue :

   ```sql
   -- scripts/sql/custom_views/v_depenses_par_centre.sql
   CREATE OR REPLACE VIEW splus.v_depenses_par_centre AS
   SELECT c.prctr            AS centre_profit,
          SUM(b.dmbtr)       AS total_depense,
          COUNT(*)           AS nb_ecritures
     FROM splus.bkpf b
     JOIN splus.cepc c ON c.prctr = b.prctr
    GROUP BY c.prctr;
   ```

   > Référencez toujours `splus.*` (les vues publiques), **jamais**
   > `splus_blue.*` ou `splus_green.*` : sinon votre vue resterait
   > accrochée à un schéma physique et serait cassée au prochain switch.

2. **Appliquer la vue** :

   ```bash
   ./manage.sh trigger amue_refresh_views
   ```

3. **Vérifier** :

   ```bash
   ./manage.sh db-shell
   ```

   ```sql
   SELECT table_name FROM information_schema.views
    WHERE table_schema = 'splus' AND table_name = 'v_depenses_par_centre';

   SELECT * FROM splus.v_depenses_par_centre LIMIT 5;
   ```

**Automatiser** la recréation après chaque import (recommandé) :

```bash
./manage.sh var-set amue_post_import_dags '["amue_refresh_views"]'
```

Ainsi, chaque import principal réussi déclenchera automatiquement
`amue_refresh_views`.

### 3.5 Modifier une vue custom existante

1. Éditez le fichier `scripts/sql/custom_views/<nom>.sql`.
2. Déclenchez `amue_refresh_views` :

   ```bash
   ./manage.sh trigger amue_refresh_views
   ```

3. Vérifiez la nouvelle définition :

   ```sql
   SELECT view_definition FROM information_schema.views
    WHERE table_schema = 'splus' AND table_name = 'v_depenses_par_centre';
   ```

Pour **supprimer** une vue custom :

1. Supprimez le fichier SQL correspondant.
2. Supprimez la vue en base :

   ```sql
   DROP VIEW IF EXISTS splus.v_depenses_par_centre;
   ```

### 3.6 Modifier l'heure d'un import

Chaque DAG planifié lit son cron depuis une variable Airflow :

| DAG                         | Variable                | Défaut        |
|-----------------------------|-------------------------|---------------|
| `amue_multi_table_import`   | `amue_import_schedule`  | `0 2 * * *`   |
| `amue_sync_schemas`         | `amue_sync_schedule`    | `0 6 * * *`   |
| `amue_status_monitor`       | `amue_monitor_schedule` | `0 22 * * *`  |
| `ecc_multi_table_import`    | `ecc_import_schedule`   | `0 4 * * *`   |

**Exemple** — décaler l'import principal à 3h15 :

```bash
./manage.sh var-set amue_import_schedule '15 3 * * *'
./manage.sh refresh-plugins       # force le scheduler à relire les DAGs
```

**Vérifier** dans l'UI que la colonne "Next run" du DAG a bien changé.

### 3.7 Ajuster les performances de l'import

Deux leviers principaux :

```bash
# Nombre de lignes récupérées par page depuis l'API
./manage.sh var-set amue_import_batch_size 10000   # défaut : 5000

# Nombre de tables importées en parallèle
./manage.sh var-set amue_import_parallel_workers 3 # défaut : 1
```

**Conseils** :

- Augmenter `amue_import_parallel_workers` accélère l'import mais charge
  plus l'API et la base : ne dépassez pas 5 sans vérifier que l'API AMUE
  le supporte (risque de HTTP 429, voir [5.5](#55-erreurs-côté-api-amue)).
- Un batch trop grand (> 20 000) fait gonfler la mémoire des tâches.

### 3.8 Changer les destinataires des rapports

```bash
./manage.sh var-set amue_report_recipients "alice@univ.fr,bob@univ.fr"
./manage.sh var-set ecc_report_recipients  "alice@univ.fr"
```

Le prochain run utilisera la nouvelle liste. Les virgules séparent les
destinataires.

### 3.9 Tester un import sans attendre l'API

Par défaut, `amue_multi_table_import` attend (via un sensor) que l'API
publie un nouveau rapport. En dev, pour débloquer un test :

```bash
./manage.sh var-set amue_force_import true
./manage.sh trigger amue_multi_table_import
# ... puis, impérativement :
./manage.sh var-set amue_force_import false
```

> **Ne jamais laisser `amue_force_import = true` en production** : vous
> importeriez le même rapport en boucle et pollueriez l'état.

### 3.10 Annuler un import (rollback)

**Quand** : vous venez de faire un import qui a corrompu les données
attendues. Le rollback repointe les vues vers le schéma précédent.

**Condition** : le rollback n'est disponible **que jusqu'au prochain
import réussi**. Au-delà, il faut restaurer depuis une sauvegarde.

**Étapes** :

1. Vérifier que le rollback est possible :

   ```bash
   ./manage.sh db-shell
   ```

   ```sql
   SELECT schema_name FROM information_schema.schemata
    WHERE schema_name LIKE '%_offline';
   ```

   Vous devez voir `splus_blue_offline` **ou** `splus_green_offline`.

2. Lancer :

   ```bash
   ./manage.sh trigger amue_rollback
   ```

3. Vérifier :

   ```sql
   SELECT active_schema, last_switch_timestamp
     FROM splus_admin.amue_state WHERE id = 1;
   ```

### 3.11 Chaîner des DAGs avant ou après l'import

Deux variables Airflow (JSON array) permettent d'exécuter d'autres DAGs
en séquence autour de `amue_multi_table_import` :

```bash
# Préparer les tables juste avant chaque import
./manage.sh var-set amue_pre_import_dags '["amue_table_setup"]'

# Rafraîchir les vues custom juste après un import réussi
./manage.sh var-set amue_post_import_dags '["amue_refresh_views"]'

# Plusieurs DAGs possibles (exécutés dans l'ordre)
./manage.sh var-set amue_post_import_dags '["amue_refresh_views","amue_sync_schemas"]'

# Désactivation
./manage.sh var-set amue_pre_import_dags  '[]'
./manage.sh var-set amue_post_import_dags '[]'
```

Dans l'UI, vous verrez apparaître des tâches `trigger_pre_import_*` et
`trigger_post_import_*` dans le graphe du DAG principal.

### 3.12 Renouveler les credentials OAuth AMUE

**Quand** : AMUE a tourné votre `client_secret`, ou vous passez de
sandbox à prod.

```bash
./manage.sh conn-update oauth_api
./manage.sh conn-test oauth_api
```

Le `conn_id` est **`oauth_api`** (pas `amue_api`).

### 3.13 Consulter les emails en dev (MailHog)

En environnement de développement, le SMTP est redirigé vers MailHog :
les emails ne partent **pas** vers l'extérieur, ils sont capturés
localement.

- UI : http://localhost:8025
- Un email = une ligne ; cliquez pour voir le contenu HTML du rapport
  d'import.
- MailHog n'envoie rien, il stocke uniquement. Pour vider la boîte :
  bouton "Delete all messages".

### 3.14 Sauvegarder la base métier

```bash
./manage.sh db-backup                              # dump horodaté
./manage.sh db-restore dump_YYYYMMDD_HHMMSS.sql    # restauration
```

Pour sauvegarder aussi la configuration Airflow (variables + connexions + .env) :

```bash
./manage.sh config-backup
```

### 3.15 Importer une table depuis Oracle ECC

**Quand** : vous avez besoin d'une table qui vit dans Oracle ECC (pas dans
l'API AMUE).

**Prérequis** : la connexion Airflow `oracle_data` doit être créée et
valide (`./manage.sh conn-test oracle_data`).

**Étapes** :

1. **Déclarer la table** avec sa requête SQL Oracle :

   ```bash
   ./manage.sh db-shell
   ```

   ```sql
   INSERT INTO splus_admin.amue_tables
     (table_name, enabled, primary_key, delta, ecc_query)
   VALUES (
     'ecc_fournisseurs',
     true,
     'ID_FOURNISSEUR',
     '',
     'SELECT id_fournisseur, raison_sociale, siret, updated_at
        FROM T_FOURNISSEURS
       WHERE actif = 1'
   )
   ON CONFLICT (table_name) DO UPDATE
   SET ecc_query = EXCLUDED.ecc_query,
       enabled   = true;
   ```

   > La présence d'un `ecc_query` non-NULL fait basculer la table sur la
   > chaîne ECC (DAG `ecc_multi_table_import`) au lieu de la chaîne AMUE.

2. **Initialiser la table** :

   ```bash
   ./manage.sh trigger amue_table_setup
   ```

3. **Lancer l'import ECC** :

   ```bash
   ./manage.sh trigger ecc_multi_table_import
   ```

4. **Vérifier** :

   ```sql
   SELECT COUNT(*) FROM splus.ecc_fournisseurs;
   ```

### 3.16 Ré-importer des tables spécifiques (correction)

**Quand** : une ou plusieurs tables ont des données incorrectes après un import, vous avez corrigé la cause, et vous voulez ré-importer uniquement ces tables sans attendre l'heure planifiée ni relancer l'import complet.

**Prérequis** : les tables ciblées doivent avoir `setup_status = 'ready'` dans `splus_admin.amue_tables`. Si ce n'est pas le cas, lancer d'abord `amue_table_setup`.

**Différences avec le DAG principal :**

- Pas de sensor (l'API est appelée directement au déclenchement)
- Import toujours en mode FULL (pas de delta)
- Seules les tables listées dans `selected_tables` sont traitées

**Étapes via l'UI Airflow :**

1. Ouvrir le DAG `amue_correction_import` (ou `ecc_correction_import` pour ECC)
2. Cliquer sur **Trigger DAG w/ config**
3. Dans le champ `selected_tables`, saisir le tableau JSON des tables à ré-importer :

   ```json
   ["CSKS", "LFA1", "BKPF"]
   ```

4. Soumettre.

**Via CLI :**

```bash
docker compose exec airflow-apiserver airflow dags trigger amue_correction_import \
  --conf '{"selected_tables": ["CSKS", "LFA1"]}'
```

**Pour ECC :**

```bash
docker compose exec airflow-apiserver airflow dags trigger ecc_correction_import \
  --conf '{"selected_tables": ["ecc_fournisseurs"]}'
```

**Vérifier** : dans la vue Grid du run, les tâches `import_data[CSKS]`, `import_data[LFA1]` doivent passer en vert. Les tables non sélectionnées ne sont pas touchées.

**Lister les tables disponibles** si vous ne vous souvenez pas des noms exacts :

```sql
-- Tables AMUE activées
SELECT table_name, setup_status FROM splus_admin.amue_tables
 WHERE enabled = TRUE ORDER BY table_name;

-- Tables ECC activées (avec ecc_query)
SELECT table_name FROM splus_admin.amue_tables
 WHERE enabled = TRUE AND ecc_query IS NOT NULL ORDER BY table_name;
```

---

### 3.17 Reprendre un import après échec partiel



**Contexte** : l'import s'est planté au milieu (ex : 25 tables sur 32
importées, puis `import_data[25]` a échoué à cause d'un timeout API).
La tâche `restore_inactive` a recopié les 25 réussies dans le schéma
inactif — vos données ne sont donc pas corrompues, mais l'import n'a
pas basculé les vues.

**Deux stratégies** :

**Stratégie A — corriger et réessayer la tâche en échec** (rapide si la
cause est transitoire) :

1. Dans l'UI, ouvrir la vue Grid du run en échec.
2. Cliquer sur la tâche rouge (ex : `import_data[25]`) → onglet
   **Logs** pour confirmer la cause.
3. Cliquer sur **Clear** → Airflow réexécute la tâche et les tâches
   en aval (`save_metadata`, `switch_views`, `send_report`).

   Équivalent CLI :
   ```bash
   docker compose exec airflow-apiserver airflow tasks clear \
     amue_multi_table_import -t import_data -s <logical_date> -e <logical_date>
   ```

**Stratégie B — repartir sur un run neuf** (si la cause demande de
ré-appeler le sensor et de retraiter depuis le début) :

```bash
./manage.sh trigger amue_multi_table_import
```

Attention : le nouveau run attendra que l'API publie **un nouveau
rapport** (sauf si vous activez `amue_force_import=true` pour le test).

### 3.18 Consulter l'historique des imports

**Dans l'UI** : DAG → vue **Grid** ou **Calendar**. Chaque colonne/jour
est un run, la couleur indique le résultat.

**En SQL** — derniers jalons réussis :

```sql
SELECT last_successful_run,
       last_report_start,
       last_switch_timestamp,
       active_schema
  FROM splus_admin.amue_state WHERE id = 1;
```

**En SQL** — fréquence et volume par table (via colonne meta) :

```sql
SELECT DATE(_imported_at) AS jour,
       COUNT(*)           AS lignes_importees
  FROM splus.bkpf
 GROUP BY DATE(_imported_at)
 ORDER BY jour DESC
 LIMIT 30;
```

**En CLI** — derniers runs Airflow :

```bash
docker compose exec airflow-apiserver airflow dags list-runs \
  -d amue_multi_table_import --limit 20
```

### 3.19 Gérer les utilisateurs Airflow

**Lister** :

```bash
./manage.sh users
```

**Créer un utilisateur** :

```bash
./manage.sh add-user
```

Le script est interactif (username, rôle, email, mot de passe). Rôles
Airflow standards : `Admin`, `Op`, `User`, `Viewer`, `Public`.

**Supprimer** :

```bash
./manage.sh delete-user <username>
```

> Changez **toujours** le mot de passe par défaut (`airflow` / `airflow`)
> avant de mettre la stack en production.

---

## 4. Comprendre les logs

### 4.1 Où regarder en premier

**Toujours l'UI Airflow d'abord** — c'est le moyen le plus rapide :

1. http://localhost:8080 → cliquer sur le DAG.
2. Vue **Grid** (par défaut) : une colonne par run, une ligne par tâche,
   chaque case colorée par statut.
3. Cliquer sur la case rouge → onglet **Logs** à droite.

La vue Grid donne immédiatement la forme de l'échec :

- **Une seule tâche rouge** (ex : `import_data[7]`) → problème localisé
  sur cette table.
- **`check_setup_status` rouge** → une ou plusieurs tables ne sont pas
  `ready` ([5.2](#52-une-table-reste-pending-ou-passe-blocked)).
- **`wait_for_api` rouge** → l'API AMUE n'a pas répondu dans les délais
  ([5.5](#55-erreurs-côté-api-amue)).
- **`init_bluegreen` rouge** → un verrou empêche le démarrage ([5.6](#56-concurrentimporterror--verrou-bloqué)).

### 4.2 Lire un log de tâche

Structure typique (du haut vers le bas) :

```
*** Reading local file: /opt/airflow/logs/dag_id=amue_multi_table_import/...
[2026-04-24 02:00:01] {taskinstance.py:...} INFO - Starting attempt 1 of 2
[2026-04-24 02:00:02] {...} INFO - Executing <Task(...)>
  ... sortie de la tâche (messages applicatifs, exceptions) ...
[2026-04-24 02:04:55] {taskinstance.py:...} ERROR - Task failed with exception
Traceback (most recent call last):
  ...
  File ".../plugins/amue/...", line XX, in ...
    raise AMUEDataError("...")
amue.exceptions.AMUEDataError: Doublons détectés dans les données API
[2026-04-24 02:04:55] {taskinstance.py:...} INFO - Marking task as FAILED
```

**Ce qui compte** :

- Le **dernier `Traceback`** — c'est la vraie cause.
- Le **type d'exception** : commence par `AMUE...` pour les erreurs du
  projet (voir [5](#5-gérer-les-erreurs)).
- Les lignes **avant** le traceback : les derniers `INFO` applicatifs
  indiquent **où** dans le traitement ça a lâché (ex : "Fetching page
  42/50", "Inserting batch of 5000 rows for table BKPF").

### 4.3 Logs en ligne de commande

Streamer les logs en direct :

```bash
./manage.sh logs                        # tous les services
./manage.sh logs airflow-scheduler      # un service en particulier
./manage.sh logs postgres-data
```

Logs d'une tâche précise (utile quand l'UI est lente) :

```bash
./manage.sh task-logs amue_multi_table_import import_data <run_id>
```

### 4.4 Exceptions AMUE à connaître

| Exception                   | Ce que ça veut dire                                      | Voir                                                  |
|-----------------------------|----------------------------------------------------------|-------------------------------------------------------|
| `AMUEDataError`             | Données invalides (doublons, PK manquante…)              | [5.1](#51-le-dag-est-rouge--par-où-commencer)         |
| `AMUEAuthError`             | Authentification OAuth impossible (401)                  | [5.5](#55-erreurs-côté-api-amue)                      |
| `AMUENetworkError`          | Timeout ou API injoignable                               | [5.5](#55-erreurs-côté-api-amue)                      |
| `AMUEAPIError`              | Erreur HTTP côté API (429, 5xx)                          | [5.5](#55-erreurs-côté-api-amue)                      |
| `AMUEDatabaseError`         | Problème PostgreSQL (connexion refusée, too many conns…) | [5.4](#54-erreurs-côté-postgresql)                    |
| `AMUEBatchError`            | Erreur sur un batch UPSERT                               | [5.4](#54-erreurs-côté-postgresql)                    |
| `ConcurrentImportError`     | Un import est déjà en cours (ou verrou abandonné)        | [5.6](#56-concurrentimporterror--verrou-bloqué)       |
| `ViewSwitchError`           | Bascule des vues ratée                                   | [5.7](#57-viewswitcherror--les-vues-nont-pas-basculé) |
| `RollbackNotAvailableError` | Pas de schéma `_offline` — rollback impossible           | [3.10](#310-annuler-un-import-rollback)               |

### 4.5 Mettre le niveau de log en DEBUG (temporaire)

Dans `.env` :

```bash
AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

Puis :

```bash
./manage.sh restart
```

**À remettre sur `INFO` après l'investigation** : DEBUG peut multiplier
par 10 le volume de logs.

---

## 5. Gérer les erreurs

### 5.1 Le DAG est rouge — par où commencer

1. **UI → Grid view** du DAG concerné, repérer la première tâche rouge.
2. **Onglet Logs** → descendre jusqu'au dernier traceback.
3. Identifier le type d'exception (tableau [4.4](#44-exceptions-amue-à-connaître)) et aller à la section
   correspondante.
4. Corriger la cause.
5. **Clear** la tâche dans l'UI (bouton "Clear") → Airflow la relancera
   automatiquement, et le reste du DAG enchaînera. Ou :

   ```bash
   docker compose exec airflow-apiserver airflow tasks clear \
     amue_multi_table_import -t <task_id>
   ```

6. Si vous voulez tout relancer à zéro : **Trigger** un nouveau run.

### 5.2 Une table reste `pending` ou passe `blocked`

**Diagnostic** :

```bash
./manage.sh list-tables
```

Ou plus détaillé :

```sql
SELECT table_name, setup_status, updated_at
  FROM splus_admin.amue_tables
 WHERE enabled = true AND setup_status <> 'ready';
```

**Cas `pending`** — la table n'a jamais été initialisée avec succès.
Consulter les logs du DAG `amue_table_setup` pour cette table :

```bash
./manage.sh task-logs amue_table_setup setup_table_<NOM> <run_id>
```

Causes typiques :

- La table n'existe pas côté API → la désactiver
  (`./manage.sh disable-table NOM`).
- Erreur réseau pendant le setup → relancer `amue_table_setup`.

**Cas `blocked`** — la structure a changé (nouvelle colonne, type modifié).
Le DAG refuse d'importer pour ne pas corrompre les données.

Option A — **accepter la nouvelle structure** :

```sql
UPDATE splus_admin.amue_tables
   SET setup_status      = 'pending',
       fingerprint_api   = '',
       fingerprint_local = '',
       updated_at        = NOW()
 WHERE table_name = 'NOM_TABLE';
```

Puis :

```bash
./manage.sh trigger amue_table_setup
```

Option B — **geler la table** le temps de clarifier avec AMUE :

```bash
./manage.sh disable-table NOM_TABLE
```

### 5.3 L'import tourne depuis des heures

**Diagnostic** :

```sql
SELECT import_in_progress,
       import_started_at,
       AGE(NOW(), import_started_at) AS duree
  FROM splus_admin.amue_state WHERE id = 1;
```

- `duree < 2 h` → attendez. Un import peut légitimement durer 1 à 2 h.
- `duree > 2 h` **et un run Airflow est encore actif** → regardez la vue
  Grid : sur quelle tâche ça coince ? Souvent `wait_for_api` (API lente)
  ou `import_data[X]` (une table énorme).
- `duree > 2 h` **mais aucun run Airflow actif** → verrou abandonné,
  [5.6](#56-concurrentimporterror--verrou-bloqué).

### 5.4 Erreurs côté PostgreSQL

**`Connection refused`** :

```bash
docker compose ps postgres-data
docker compose logs postgres-data --tail 50
docker compose restart postgres-data
```

**`Too many connections`** :

```sql
SELECT count(*), state FROM pg_stat_activity GROUP BY state;

-- Tuer les idle > 1h
SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
 WHERE state = 'idle'
   AND query_start < now() - interval '1 hour';
```

**`schema "splus_blue" does not exist`** — l'init n'a pas tourné :

```bash
docker compose exec postgres-data psql -U datauser -d business_data \
  -f /scripts/sql/init_db.sql
```

### 5.5 Erreurs côté API AMUE

**HTTP 401 (`AMUEAuthError`)** — credentials invalides :

```bash
./manage.sh conn-update oauth_api
./manage.sh conn-test oauth_api
```

**HTTP 429 (rate limiting)** — vous appelez trop vite :

```bash
./manage.sh var-set amue_api_retry_delay_seconds 60
./manage.sh var-set amue_import_parallel_workers 1
```

**HTTP 500/502/503** — erreur serveur AMUE. Attendez, relancez. Si
persistant, contactez le support AMUE.

**Timeout / `AMUENetworkError`** :

```bash
# Tester la connectivité
curl -v https://sandbox.api.amue.fr/health
```

### 5.6 `ConcurrentImportError` — verrou bloqué

Symptôme : impossible de lancer un nouvel import, le DAG échoue tout de
suite sur `init_bluegreen`.

**Diagnostic** :

```sql
SELECT import_in_progress,
       import_started_at,
       AGE(NOW(), import_started_at) AS duree,
       last_successful_run
  FROM splus_admin.amue_state WHERE id = 1;
```

Si `duree > 2 h` et qu'**aucun** run Airflow n'est réellement actif
(vérifier la vue Grid : pas de tâche en cours), libérez le verrou :

```bash
docker compose exec airflow-apiserver python -c "
from common.services.bluegreen.bluegreen_manager import BlueGreenManager
BlueGreenManager()._force_release_lock()
print('Verrou libéré')
"
```

Dernier recours (SQL direct, à **ne pas** utiliser si un import tourne
vraiment) :

```sql
UPDATE splus_admin.amue_state
   SET import_in_progress = false, updated_at = NOW()
 WHERE id = 1;
```

### 5.7 `ViewSwitchError` — les vues n'ont pas basculé

Inspecter les vues actuelles :

```sql
SELECT table_name, view_definition
  FROM information_schema.views
 WHERE table_schema = 'splus';
```

Si certaines pointent encore vers `splus_blue` alors que d'autres pointent
vers `splus_green` → schémas désynchronisés :

```bash
./manage.sh trigger amue_sync_schemas
```

En dernier recours (recréation manuelle d'une vue) :

```sql
DROP VIEW  IF EXISTS splus.csks;
CREATE VIEW splus.csks AS SELECT * FROM splus_blue.csks;  -- ou splus_green
```

### 5.8 Un DAG n'apparaît pas dans l'UI

```bash
docker compose exec airflow-apiserver airflow dags list-import-errors
```

- Erreur de syntaxe Python → lire le fichier et la ligne indiqués.
- Dépendance manquante → compléter `requirements.txt` puis
  `docker compose build && ./manage.sh restart`.
- DAG ajouté il y a < 1 min → attendre le `dag-processor`
  (`./manage.sh refresh-plugins` pour accélérer).

### 5.9 Une tâche reste bloquée en "running"

1. Ouvrir les logs de la tâche dans l'UI : souvent ils révèlent une
   attente (API, DB, lock).
2. Vérifier que les conteneurs tournent : `./manage.sh status`.
3. Forcer le clear :

   ```bash
   docker compose exec airflow-apiserver airflow tasks clear \
     <dag_id> -t <task_id>
   ```

### 5.10 Emails de rapport non reçus

**En dev** : ils partent vers MailHog ([3.13](#313-consulter-les-emails-en-dev-mailhog)). Si MailHog est vide :

```bash
docker compose ps mailhog
```

**En prod** : vérifier les variables SMTP :

```bash
./manage.sh var-get smtp_host
./manage.sh var-get smtp_port
./manage.sh var-get smtp_use_tls
./manage.sh var-get amue_report_recipients
```

### 5.11 Remettre tout à plat en cas de doute

```bash
./manage.sh diagnose
```

Produit un rapport complet (services, versions, variables, connexions,
derniers runs, erreurs de parsing). À joindre à toute demande de support.

---

## 6. Superviser l'état du système

### 6.1 Checklist quotidienne (5 minutes)

- [ ] `./manage.sh health` — tous les services OK ?
- [ ] UI Airflow — dernier run de `amue_multi_table_import` vert ?
- [ ] MailHog (ou boîte prod) — rapport reçu ?
- [ ] Aucune table `blocked` ?

  ```sql
  SELECT count(*) FROM splus_admin.amue_tables WHERE setup_status = 'blocked';
  ```

- [ ] Espace disque sur l'hôte > 20 % libre (`df -h`).

### 6.2 Requêtes de supervision utiles

```bash
./manage.sh db-shell
```

```sql
-- État global
SELECT active_schema,
       import_in_progress,
       last_successful_run,
       last_switch_timestamp
  FROM splus_admin.amue_state WHERE id = 1;

-- Statut des tables (surveillance des `blocked` / `pending`)
SELECT table_name, enabled, setup_status, updated_at
  FROM splus_admin.amue_tables
 WHERE enabled = true
 ORDER BY setup_status DESC, table_name;

-- Volumétrie par table
SELECT schemaname, relname, n_live_tup
  FROM pg_stat_user_tables
 WHERE schemaname IN ('splus_blue', 'splus_green')
 ORDER BY n_live_tup DESC
 LIMIT 20;

-- Date du dernier import par table (via colonne meta)
SELECT MAX(_imported_at) AS last_import
  FROM splus.csks;
```

### 6.3 Derniers runs en ligne de commande

```bash
./manage.sh dags                   # tous les DAGs
./manage.sh failed 10              # 10 dernières tâches en échec
docker compose exec airflow-apiserver airflow dags list-runs \
  -d amue_multi_table_import --limit 10
```

---

## 7. Référence rapide

### 7.1 Commandes `./manage.sh` par fréquence d'usage

**Tous les jours** :

```bash
./manage.sh health
./manage.sh trigger <dag_id>
./manage.sh failed 10
./manage.sh logs [service]
```

**Quand on ajoute/retire une table** :

```bash
./manage.sh list-tables
./manage.sh add-table <t>
./manage.sh disable-table <t>
./manage.sh remove-table <t>
```

**Quand on change la config** :

```bash
./manage.sh var-get <key>
./manage.sh var-set <key> <val>
./manage.sh conn-update <id>
./manage.sh refresh-plugins
```

**Pour investiguer** :

```bash
./manage.sh db-shell
./manage.sh task-logs <dag> <task> <run>
./manage.sh diagnose
```

**Pour maintenir** :

```bash
./manage.sh cleanup-logs 30
./manage.sh cleanup-db 30
./manage.sh db-backup
./manage.sh config-backup
```

### 7.2 Variables Airflow les plus utilisées

| Variable                             | À quoi elle sert                    | Défaut       |
|--------------------------------------|-------------------------------------|--------------|
| `amue_import_schedule`               | Heure de l'import principal         | `0 2 * * *`  |
| `amue_sync_schedule`                 | Heure de la sync blue/green         | `0 6 * * *`  |
| `amue_force_import`                  | Bypass du sensor (dev uniquement)   | `false`      |
| `amue_import_batch_size`             | Lignes par appel API                | `5000`       |
| `amue_import_parallel_workers`       | Tables importées en parallèle       | `1`          |
| `amue_api_retry_delay_seconds`       | Délai entre retries API             | `30`         |
| `amue_pre_import_dags`               | DAGs à chaîner avant l'import       | `[]`         |
| `amue_post_import_dags`              | DAGs à chaîner après l'import       | `[]`         |
| `amue_report_recipients`             | Destinataires des rapports AMUE     | —            |
| `ecc_report_recipients`              | Destinataires des rapports ECC      | —            |

Liste complète : `config/airflow_variables.json`.

### 7.3 Les 3 connexions Airflow

| conn_id          | Type      | Rôle                                    |
|------------------|-----------|-----------------------------------------|
| `oauth_api`      | HTTP      | API AMUE (OAuth2 client_credentials)    |
| `postgres_data`  | Postgres  | Base métier (`splus*`) sur port 5433    |
| `oracle_data`    | ODBC      | Oracle ECC (optionnel)                  |

> Le `conn_id` AMUE est `oauth_api` (**pas** `amue_api`).
