# Concepts DemoDAGS

Référence centralisée des concepts partagés par `DEPLOYMENT.md`,
`OPERATIONS.md`, `TROUBLESHOOTING.md` et `technique.md`. Les sections détaillées
ci-dessous doivent être **la seule source de vérité** ; les autres docs
renvoient vers ce fichier pour éviter toute divergence.

---

## Statuts de table

Chaque ligne de `splus_admin.amue_tables` porte un `setup_status` qui pilote
le comportement des DAGs :

| Statut    | Signification                                                    | Conséquence                                                |
|-----------|------------------------------------------------------------------|------------------------------------------------------------|
| `pending` | Jamais initialisée (valeur par défaut à la création).            | `amue_table_setup` doit traiter la table avant l'import.   |
| `ready`   | Initialisée avec succès, fingerprints à jour, prête pour import. | `amue_multi_table_import` peut l'importer.                 |
| `blocked` | Changement de structure détecté (fingerprint mismatch).          | Intervention manuelle requise avant de débloquer l'import. |

Voir `docs/TROUBLESHOOTING.md` pour les procédures de déblocage (forçage en
`pending`, désactivation temporaire, etc.).

---

## Schémas blue/green

Le projet maintient **deux schémas de tables** (`splus_blue` et `splus_green`) et
un schéma de vues (`splus`) qui pointe atomiquement vers l'un ou l'autre.

- L'état (schéma actif, verrou d'import, timestamp de dernier import) est persisté
  dans `splus_admin.amue_state` — **pas** dans des variables Airflow.
- Le basculement de vues est atomique (DROP + CREATE dans une même transaction).
- Un rollback reste possible jusqu'au prochain import (DAG `amue_rollback`).

---

## Vérifier PostgreSQL

Séquence standard pour valider une installation ou diagnostiquer un incident :

```bash
# 1. Service actif
systemctl status postgresql

# 2. Connexion depuis le host Airflow
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT version();"

# 3. Schémas en place
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "\\dn"
# Attendu : splus, splus_admin, splus_blue, splus_green

# 4. Table d'état blue/green
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT * FROM splus_admin.amue_state;"

# 5. Nombre de connexions actives (utile si "too many clients")
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT count(*) FROM pg_stat_activity;"
```

---

## Vérifier la connexion Airflow → PostgreSQL

```bash
airflow connections get postgres_data
# Champ 'host', 'schema', 'login' doivent correspondre à l'environnement cible
```

Le `schema` dans la connexion Airflow est ignoré : chaque `PostgresHook` est
créé avec `options='-c search_path=<schema>'` via `create_postgres_hook()` /
`resolve_postgres_hook()` (voir `plugins/common/infrastructure/database/hooks.py`).
