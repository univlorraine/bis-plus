# Migrations SQL applicatives

Ce répertoire contient les **migrations** du schéma applicatif (`splus_admin`,
`splus_blue`/`splus_green`, etc.), c'est-à-dire les évolutions du schéma à appliquer
lors d'une montée de version (ajout de colonne, nouvel index, nouvelle table...).

Les migrations sont suivies dans la table `splus_admin.schema_migrations` et appliquées
via `./manage.sh db-migrate` (appelé automatiquement par `./manage.sh update`).

## Convention de nommage

```
NNNN_description_courte.sql
```

- `NNNN` : séquence sur 4 chiffres, strictement croissante (`0001`, `0002`, ...)
- `description_courte` : en minuscules, mots séparés par `_`

Exemple : `0001_add_amue_tables_priority_column.sql`

Les fichiers sont appliqués dans l'**ordre numérique**, un par un. Une migration est
considérée comme "en attente" si son numéro `NNNN` n'a pas de ligne correspondante
dans `splus_admin.schema_migrations.version`.

## Règle fondamentale : chaque migration doit être idempotente

`./manage.sh db-migrate` peut être rejoué sans risque (ex. après une erreur en cours
de mise à jour) — c'est ce qui rend la reprise possible sans mécanisme de "rollback de
migration". Pour cela, chaque fichier doit pouvoir être exécuté plusieurs fois sans erreur
ni effet de bord, exactement comme le sont les schémas/tables/vues du Blue/Green
(`CREATE SCHEMA IF NOT EXISTS`, `INSERT ... ON CONFLICT DO NOTHING`, `CREATE OR REPLACE VIEW`).

Patterns à utiliser :

```sql
-- Ajout de colonne
ALTER TABLE splus_admin.amue_tables
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;

-- Index
CREATE INDEX IF NOT EXISTS idx_amue_tables_priority
    ON splus_admin.amue_tables (priority);

-- Opérations sans variante "IF NOT EXISTS" (ex. ajout de contrainte) : encadrer
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'amue_tables_priority_check'
    ) THEN
        ALTER TABLE splus_admin.amue_tables
            ADD CONSTRAINT amue_tables_priority_check CHECK (priority >= 0);
    END IF;
END $$;
```

## Anti-pattern à éviter

```sql
-- MAUVAIS : échoue si rejoué (la colonne existe déjà)
ALTER TABLE splus_admin.amue_tables ADD COLUMN priority INTEGER;

-- MAUVAIS : TRUNCATE/DELETE — le projet ne supprime jamais de données (UPSERT only)
TRUNCATE splus_admin.amue_tables;
```

## Pas de "down migration"

Il n'y a volontairement aucun mécanisme pour annuler une migration individuellement
(pas de fichier `NNNN_down.sql`, pas de checksum, pas de flag "dirty" à la Flyway/Alembic
— hors de proportion pour ce projet). En cas de problème après une mise à jour, on revient
en arrière au niveau du **projet dans son ensemble** (code + base + configuration), voir
[`docs/UPGRADE.md`](../../../docs/UPGRADE.md#rollback).

## Ajouter une migration

1. Créer le fichier `NNNN_description.sql` avec le numéro suivant disponible
2. Écrire un SQL idempotent (cf. patterns ci-dessus)
3. Tester localement : `./manage.sh db-migrate` (rejouer la commande doit être un no-op)
4. La migration sera appliquée automatiquement lors de la prochaine `./manage.sh update`
