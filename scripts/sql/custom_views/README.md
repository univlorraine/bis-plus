# Vues personnalisées (custom views)

Ce répertoire contient les définitions SQL des **vues personnalisées** du schéma `splus`.
Ces vues sont recréées automatiquement à chaque switch blue/green, dans la même transaction
atomique que les vues auto-générées.

## Règle fondamentale : pas de vues-de-vues

Chaque vue custom doit référencer **uniquement `{target_schema}.<table>`** — des tables
réelles dans `splus_blue` ou `splus_green`.

Il est **interdit** de référencer `splus.<vue>` (une vue auto-générée), car cela crée
une vue-sur-une-vue, ce qui rompt l'atomicité du switch.

## Convention

- Le placeholder `{target_schema}` est remplacé à l'exécution par `splus_blue` ou `splus_green`
- Les fichiers sont exécutés dans l'**ordre alphabétique** (déterministe, mais sans importance
  puisqu'il n'y a pas de dépendances entre vues)
- Commencer par `DROP VIEW IF EXISTS splus.<nom>;` puis `CREATE VIEW splus.<nom> AS ...`
- **Ne jamais** inclure de `COMMIT` dans les fichiers (la transaction est gérée par le switcher)

## Exemple correct

```sql
-- example.sql
DROP VIEW IF EXISTS splus.reporting_csks;
CREATE VIEW splus.reporting_csks AS
    SELECT c.bukrs, c.kostl, c.datbi, p.posid
    FROM {target_schema}.csks c
    JOIN {target_schema}.prps p ON c.bukrs = p.bukrs
    WHERE c.datbi > CURRENT_DATE;
```

## Anti-pattern à éviter

```sql
-- MAUVAIS : référence splus.csks qui est une vue auto-générée
CREATE VIEW splus.bad_view AS
    SELECT * FROM splus.csks;  -- vue-de-vue
```
