# Fonctionnement — Base intermédiaire SifacPlus

## Vue d'ensemble

SifacPlus importe quotidiennement les données financières depuis l'API SIFAC+ et ECC vers une base PostgreSQL. Chaque import est non-destructif (UPSERT uniquement), traçable, et réversible.

---

## Cycle d'un import

Un import se déroule en 5 phases enchaînées par Apache Airflow.

### Phase 1 — Initialisation Blue/Green

Le DAG détermine le schéma cible (opposé du schéma actuellement actif : `splus_blue` ou `splus_green`) et pose un verrou atomique pour éviter les imports concurrents.

### Phase 2 — Polling & sélection des tables

Un capteur interroge l'API SIFAC+ jusqu'à ce qu'un nouveau rapport soit disponible. Une fois disponible, les tables activées dans la configuration sont sélectionnées et associées au schéma cible.

### Phase 3 — Vérification de structure

Avant tout import, la structure de chaque table est vérifiée via un double fingerprint :

- **fingerprint_API** — empreinte calculée sur les types et clés fournis par l'API
- **fingerprint_UNIV** — empreinte calculée sur la structure PostgreSQL réelle

Si une divergence est détectée (changement de colonne, de type…), le DAG s'arrête et envoie une alerte email. L'import ne reprend qu'après validation manuelle.

### Phase 4 — Import (parallèle)

Les tables sont importées en parallèle (jusqu'à 10 simultanément). Chaque table est traitée par pages successives. Les données sont écrites dans le schéma cible via UPSERT (`INSERT ON CONFLICT UPDATE`) — **aucune ligne n'est jamais supprimée**. Chaque ligne importée porte les colonnes `_source` et `_imported_at`.

L'import peut être **complet** (toutes les lignes) ou **différentiel** (uniquement les lignes modifiées depuis le dernier import, via une colonne de date configurée).

### Phase 5 — Switch & finalisation

Une fois toutes les tables importées, les vues du schéma `splus` sont basculées atomiquement vers le nouveau schéma (toutes ou aucune). L'ancien schéma devient le snapshot de rollback. Un rapport email est envoyé.

---

## Architecture Blue/Green

```
splus_blue   ←──── import en cours
splus_green  ←──── schéma actif (servi via les vues splus.*)

             après switch :

splus_blue   ←──── schéma actif (servi via les vues splus.*)
splus_green  ←──── snapshot disponible pour rollback
```

Les applicatifs consommateurs n'accèdent jamais directement à `splus_blue` ou `splus_green` : ils utilisent uniquement les vues du schéma `splus`, dont la cible change de façon transparente à chaque import.

---

## Rollback

Si des données incorrectes sont détectées après un import réussi, il est possible de revenir instantanément à l'état précédent :

```bash
./manage.sh trigger amue_rollback
```

Le rollback rebascule les vues vers l'ancien schéma en moins d'une seconde. Il reste disponible jusqu'au prochain import.

---

## Notifications

Un email HTML est envoyé à chaque fin d'exécution (succès ou erreur), avec :
- le nombre de lignes importées par table
- les éventuelles erreurs et actions recommandées
- le schéma actif après switch

---

## Retry

En cas d'erreur lors des appels à l'API, le comportement de retry s'adapte au code HTTP :

| Code       | Tentatives | Stratégie                                      |
|------------|------------|------------------------------------------------|
| 4xx        | 0          | Pas de retry (erreur client)                   |
| 429        | 5          | Backoff exponentiel 2s → 4s → 8s → 16s → 30s  |
| 5xx        | 3          | Backoff exponentiel 5s → 10s → 20s             |
| Timeout    | 2          | Délai fixe 3s                                  |
| Connexion  | 3          | Backoff exponentiel 5s → 10s → 20s             |

---

## DAGs disponibles

| DAG                      | Rôle                                               |
|--------------------------|----------------------------------------------------|
| `dag_amue_dynamic_table` | Import principal SIFAC+ (32+ tables)               |
| `dag_ecc_dynamic_table`  | Import ECC / Oracle                                |
| `dag_amue_sync`          | Synchronisation des deux schémas Blue/Green        |
| `dag_amue_rollback`      | Rollback manuel vers le schéma précédent           |
| `dag_amue_table_setup`   | Vérification et création des tables (appelé automatiquement) |
| `dag_amue_status_monitor`| Consultation de l'état courant                     |
