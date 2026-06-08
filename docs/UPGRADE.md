# Guide de mise à jour DemoDAGS

Ce document décrit comment mettre à jour le **projet dans son ensemble** entre deux
releases : code, dépendances Python (image Docker), schéma SQL applicatif (`splus_admin`,
Blue/Green), métadonnées Airflow et variables Airflow.

## Table des matières

1. [Convention de versionning](#convention-de-versionning)
2. [Procédure automatisée](#procédure-automatisée)
3. [Procédure manuelle](#procédure-manuelle)
4. [Écrire une migration SQL](#écrire-une-migration-sql)
5. [Rollback](#rollback)

---

## Convention de versionning

- La version applicative du projet est indiquée dans le fichier **`VERSION`** à la racine
  (ex. `1.2.0`), visible aussi via `./manage.sh version`.
- Les releases sont publiées sur le dépôt **GitHub** du projet (`github.com/univlorraine/bis-plus`,
  remote `comunity` — à ne pas confondre avec `origin`, qui pointe vers le GitLab interne).
- **Une mise à jour cible toujours une release GitHub publiée** — jamais une branche, un
  commit ou un tag git "ordinaire". C'est ce que vérifie `./manage.sh update` avant d'agir
  (voir ci-dessous).

---

## Procédure automatisée

```bash
./manage.sh update            # prend automatiquement la dernière release publiée
./manage.sh update v1.2.0     # cible une release précise (doit être publiée sur GitHub)
./manage.sh update v1.2.0 --resume   # reprend une mise à jour interrompue
```

La commande déroule **9 étapes**, chacune affichée avec sa progression `[ÉTAPE N/9]` :

| # | Étape | Détail |
|---|-------|--------|
| 1 | Résolution + pré-checks | Vérifie que la cible est une release GitHub publiée (sinon refus), que l'arbre git est propre, que le tag existe localement (`git fetch --tags`), et relève l'état de santé courant |
| 2 | Confirmation | Affiche version actuelle → version cible, le nombre de migrations en attente, et demande confirmation explicite |
| 3 | Sauvegardes | `db-backup` (dump PostgreSQL) puis `config-backup` (variables, connexions, `.env`, `config/*.json`) — chemins enregistrés pour le rollback |
| 4 | Arrêt des services | `cmd_stop` |
| 5 | Mise à jour du code | `git fetch --tags && git checkout <release>` |
| 6 | Reconstruction de l'image | `cmd_build` — couvre **à la fois** le code et les dépendances Python (`requirements.txt` est installé dans l'image, pas dans un venv séparé) |
| 7 | Redémarrage | `cmd_start`. Le service `airflow-init` a `_AIRFLOW_DB_MIGRATE: 'true'` dans `docker-compose.yml` : **`airflow db migrate` se déclenche automatiquement** au démarrage, pas besoin de l'invoquer séparément |
| 8 | Migrations + variables | `db-migrate` (migrations SQL applicatives en attente, cf. ci-dessous) puis synchronisation **diff-based** des variables Airflow depuis `config/airflow_variables.json` (n'écrase jamais une valeur personnalisée par l'opérateur ; n'ajoute/ne met à jour que ce qui a changé dans le référentiel) |
| 9 | Vérification finale | `cmd_health` + `cmd_verify`, résumé (ancienne/nouvelle version, sauvegardes conservées) |

### En cas d'échec en cours de route

Le script écrit un fichier marqueur `backups/.update_in_progress` après chaque étape réussie
(numéro d'étape, chemins des sauvegardes, référence git précédente). En cas d'erreur, il
affiche directement :

- comment **reprendre** après correction : `./manage.sh update <release> --resume`
  (les étapes déjà validées sont sautées — les migrations et la synchro des variables sont
  de toute façon rejouables sans risque)
- comment **annuler complètement** : revenir à l'ancienne référence git, restaurer la base
  et la configuration depuis les sauvegardes créées à l'étape 3, reconstruire et redémarrer

Le marqueur est supprimé automatiquement à la fin d'une mise à jour réussie.

---

## Procédure manuelle

Si `./manage.sh update` lui-même est en cause (ou pour comprendre/auditer ce qu'il fait),
voici l'équivalent étape par étape avec les sous-commandes existantes :

```bash
# 1. Vérifier que la cible est bien une release publiée
#    -> consulter https://github.com/univlorraine/bis-plus/releases

# 2. Sauvegardes
./manage.sh db-backup
./manage.sh config-backup

# 3. Arrêt
./manage.sh stop

# 4. Code
git fetch --tags
git checkout <tag-de-la-release>

# 5. Image (code + dépendances)
./manage.sh build

# 6. Redémarrage (déclenche automatiquement `airflow db migrate` via airflow-init)
./manage.sh start

# 7. Migrations SQL applicatives + variables
./manage.sh db-migrate
#    Variables : comparer config/airflow_variables.json à l'existant et appliquer les
#    nouvelles clés / valeurs modifiées avec `./manage.sh var-set <clé> <valeur>`
#    (ne PAS faire `var-import` qui écrase tout, y compris vos personnalisations)

# 8. Vérification
./manage.sh health
./manage.sh verify
```

---

## Écrire une migration SQL

Les migrations vivent dans `scripts/sql/migrations/` (voir
[`README.md`](../scripts/sql/migrations/README.md) du dossier pour le détail) :

- Nommage : `NNNN_description_courte.sql` (séquence sur 4 chiffres, ordre d'application)
- **Idempotentes obligatoirement** (`ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`,
  blocs `DO $$ ... END $$;` pour les opérations sans variante `IF NOT EXISTS`) — c'est ce qui
  permet à `./manage.sh db-migrate` (et donc `./manage.sh update --resume`) d'être rejoué
  sans risque après une interruption
- Suivies dans `splus_admin.schema_migrations` (une ligne par migration appliquée, avec
  description, date et utilisateur)
- Pas de "down migration" : pour annuler une mise à jour, on revient en arrière au niveau
  du projet entier (voir [Rollback](#rollback)), pas migration par migration

---

## Rollback

Il existe **deux mécanismes complémentaires**, qui ne se substituent pas l'un à l'autre :

### 1. Problème lié à un import récent (données) → DAG `amue_rollback`

- À utiliser quand un import a corrompu/dégradé les données mais que le **code et le schéma
  sont sains**
- Se déclenche manuellement depuis l'UI Airflow : bascule les vues `splus.*` vers le schéma
  Blue/Green "offline" précédent
- **Limité dans le temps** : l'ancien schéma offline est écrasé au prochain import réussi
- Ne touche **ni** au code, **ni** aux migrations SQL (`schema_migrations`), **ni** à la
  base de métadonnées Airflow — un problème de release n'est pas résolu par ce DAG

### 2. Mauvaise release (code, schéma, dépendances) → rollback complet du projet

C'est le filet de sécurité que `amue_rollback` ne peut pas fournir, car il ignore tout de
l'historique des migrations, du code et des métadonnées Airflow :

```bash
./manage.sh stop
git checkout <ancien_tag_ou_release>
./manage.sh db-restore <backup_db.sql>          # chemin affiché lors de la mise à jour (étape 3)
./manage.sh config-restore <backup_config.tar.gz>
./manage.sh build
./manage.sh start
```

> Les chemins des sauvegardes créées par `./manage.sh update` sont rappelés dans le message
> d'erreur en cas d'échec, et dans le résumé final en cas de succès (utile si un problème
> n'apparaît qu'après coup).

### Quand utiliser lequel ?

| Symptôme | Mécanisme |
|----------|-----------|
| Données incohérentes après un import récent, code/schéma OK | DAG `amue_rollback` |
| Erreurs après une mise à jour (code, dépendances, schéma, migrations) | Rollback complet du projet |
| Les deux à la fois | Rollback complet du projet (il restaure aussi les données via le dump PostgreSQL) |

---

Voir aussi : [DEPLOYMENT.md](DEPLOYMENT.md) (installation initiale), [TROUBLESHOOTING.md](TROUBLESHOOTING.md) (dépannage courant).
