# Présentation Projet AMUE Import
## État des Lieux & Démonstrations

---

# Vue d'Ensemble

## Qu'est-ce que ce projet ?

Un système automatisé qui :
- **Récupère** les données depuis l'API AMUE (finances universitaires)
- **Vérifie** la qualité et la cohérence des données
- **Importe** dans notre base PostgreSQL
- **Notifie** l'équipe en cas de succès ou d'échec

## Chiffres Clés

| Métrique | Valeur |
|----------|--------|
| Tables gérées | 4 (CSKS, COVP, CEPC, EKET) |
| Services Docker | 7 |
| Lignes de code | ~5700 |
| Documentation | 5 fichiers |

---

# Points Positifs

## 1. Installation Simplifiée

**Avant** : Configuration manuelle complexe, plusieurs heures
**Maintenant** : Une seule commande

```bash
./manage.sh setup
```

Installation complète en ~3 minutes avec configuration automatique.

---

## 2. Gestion Intelligente des Erreurs

Le système détecte et gère automatiquement :

| Situation | Comportement |
|-----------|--------------|
| API indisponible | Attend et réessaie (jusqu'à 6h) |
| Erreur réseau | Retry automatique (3 tentatives) |
| Table manquante | Notification + arrêt propre |
| Changement structure | Détection + alerte |

---

## 3. Import Différentiel

**Gain de performance majeur**

| Type | Description | Exemple |
|------|-------------|---------|
| Import Complet | Toutes les données | 1ère exécution |
| Import Différentiel | Uniquement les changements | Exécutions suivantes |

Exemple : Table EKET avec 100 000 lignes
- Import complet : ~10 minutes
- Import différentiel : ~30 secondes (si 500 modifications)

---

## 4. Notifications Email Riches

Emails HTML formatés avec :
- Résumé visuel du statut
- Détail par table (lignes importées)
- Temps d'exécution
- Actions recommandées en cas d'erreur

---

## 5. Traçabilité Complète

Chaque exécution enregistre :
- Empreinte (fingerprint) de la structure
- Date du dernier import réussi
- Nombre de lignes traitées
- Historique des 7 derniers jours

---

## 6. Sécurité Production

| Environnement | Création table | Modification structure |
|---------------|----------------|------------------------|
| Développement | Autorisée | Autorisée |
| Production | Interdite | Interdite |

Protection contre les modifications accidentelles en production.

---

## 7. Architecture Modulaire

Code organisé par responsabilité :

```
plugins/amue/
├── hooks/       → Connexion API
├── operators/   → Opérations métier
├── services/    → Services transverses
├── notifications/ → Alertes
└── utils/       → Utilitaires partagés
```

Facilite la maintenance et les évolutions.

---

# Points à Améliorer

## 1. Pas de Tests Automatisés

**Risque** : Régression non détectée lors de modifications

**Impact** :
- Bugs découverts en production
- Temps de debug plus long
- Confiance limitée lors des déploiements

**Recommandation** : Ajouter tests unitaires et d'intégration

---

## 2. Pas de Monitoring

**Actuellement** : Vérification manuelle des logs

**Manques** :
- Pas d'alertes temps réel
- Pas de métriques de performance
- Pas de tableau de bord visuel

**Recommandation** : Intégrer Prometheus + Grafana

---

## 3. Configuration Dispersée

**Actuellement** : Variables Airflow + fichiers JSON + .env

**Problèmes** :
- Difficile de savoir où modifier un paramètre
- Risque d'incohérence entre environnements

**Recommandation** : Centraliser dans un fichier unique par environnement

---

## 4. Pas de Gestion Multi-Environnements

**Actuellement** : Un seul environnement configuré à la fois

**Manques** :
- Pas de profils (dev/preprod/prod)
- Déploiement manuel pour changer d'environnement

**Recommandation** : Fichiers de configuration par environnement

---

## 5. Documentation Technique Dense

**Actuellement** : INSTALL.md de 227 KB

**Problèmes** :
- Trop long pour une lecture rapide
- Mélange installation et référence
- Pas de vidéos/captures d'écran

**Recommandation** : Séparer en guides courts + référence

---

## 6. Gestion des Secrets

**Actuellement** : Credentials dans fichiers JSON

**Risques** :
- Commit accidentel possible
- Pas de rotation automatique

**Recommandation** : Utiliser un gestionnaire de secrets (Vault, AWS Secrets Manager)

---

# Cas d'Échec - Démonstrations

## Démo 1 : Table Absente de l'API

### Scénario
Une table configurée (ex: "ZZZZ") n'existe pas dans l'API AMUE.

### Comment reproduire

1. Modifier la configuration :
```bash
# Dans Airflow UI > Admin > Variables > amue_tables_to_import
# Ajouter une table fictive
```

2. Déclencher le DAG :
```bash
./manage.sh trigger amue_multi_table_import
```

### Résultat Attendu
- DAG échoue à l'étape "filter_tables_to_process"
- Email de notification envoyé
- Message d'erreur clair :
  ```
  ERREUR CRITIQUE: 1 table(s) configurée(s) absente(s) du statut API
  Tables manquantes: ZZZZ
  ```

### Vérification
```bash
# Voir les logs
./manage.sh logs airflow-scheduler

# Voir l'email dans MailHog
# Ouvrir http://localhost:8025
```

---

## Démo 2 : API Indisponible (Timeout)

### Scénario
L'API AMUE ne répond pas (maintenance, panne réseau).

### Comment reproduire

1. Couper l'accès réseau ou modifier l'endpoint :
```bash
# Dans Airflow UI > Admin > Variables
# Modifier api_endpoint_table avec une URL invalide
```

2. Déclencher le DAG

### Résultat Attendu
- Le système attend et réessaie
- Affichage progression :
  ```
  [POLLING] Tentative 1/36 - Attente 10min
  [POLLING] Tentative 2/36 - Attente 10min
  ...
  ```
- Après 6h (configurable) : échec avec notification

### Points Clés
- Pas de spam de requêtes (intervalle respecté)
- Backoff exponentiel optionnel
- Timeout configurable

---

## Démo 3 : Changement de Structure

### Scénario
La structure d'une table a changé côté AMUE (nouvelle colonne, type modifié).

### Comment reproduire

1. Modifier manuellement le fingerprint enregistré :
```bash
# Dans Airflow UI > Admin > Variables > amue_tables_to_import
# Modifier le finger_print d'une table
```

2. Déclencher le DAG

### Résultat Attendu
- Détection du changement de structure
- En production : arrêt avec alerte
- En développement : recréation de la table

### Message Type
```
Structure changée pour CSKS
Ancien fingerprint: abc123...
Nouveau fingerprint: def456...
Action: Validation manuelle requise
```

---

## Démo 4 : Erreur d'Insertion (Contrainte Violée)

### Scénario
Les données reçues violent une contrainte de la base (clé primaire dupliquée, valeur NULL interdite).

### Comment reproduire

1. Créer une contrainte stricte sur une table
2. Envoyer des données invalides (simuler via modification locale)

### Résultat Attendu
- Rollback automatique du batch
- Aucune donnée partielle insérée
- Erreur claire avec contexte :
  ```
  Erreur insertion CSKS après 5000 lignes:
  duplicate key value violates unique constraint
  ```

### Points Clés
- Transaction atomique par batch
- Pas de corruption de données
- Possibilité de reprendre après correction

---

## Démo 5 : Échec Sauvegarde Métadonnées

### Scénario
Après un import réussi, la sauvegarde des métadonnées échoue (base Airflow indisponible).

### Comment reproduire

1. Stopper temporairement la base Airflow après l'import
2. Observer le comportement

### Résultat Attendu (Après correction récente)
- 3 tentatives avec backoff exponentiel
- Si échec persistant : **DAG échoue**
- Raison : Éviter les imports différentiels incorrects

### Avant vs Après Correction

| Avant | Après |
|-------|-------|
| Avertissement simple | Erreur bloquante |
| Import suivant potentiellement incorrect | Import suivant garanti correct |

---

# Commandes Utiles pour les Démos

```bash
# Démarrer l'environnement
./manage.sh start

# Voir le statut
./manage.sh status

# Déclencher un DAG
./manage.sh trigger amue_multi_table_import

# Voir les logs en temps réel
./manage.sh logs airflow-scheduler -f

# Accéder à l'interface Airflow
# http://localhost:8080 (admin/admin)

# Accéder aux emails (MailHog)
# http://localhost:8025

# Shell PostgreSQL données
./manage.sh db-shell

# Diagnostic complet
./manage.sh diagnose
```

---

# Résumé

## Ce qui Fonctionne Bien

| Fonctionnalité | Maturité |
|----------------|----------|
| Import automatique | Stable |
| Gestion erreurs | Robuste |
| Notifications | Fonctionnel |
| Installation | Simple |
| Sécurité production | En place |

## À Améliorer en Priorité

| Amélioration | Effort | Impact |
|--------------|--------|--------|
| Tests automatisés | Moyen | Élevé |
| Monitoring | Moyen | Élevé |
| Multi-environnements | Faible | Moyen |
| Gestion secrets | Moyen | Élevé |

---

# Questions ?

Contact : [Votre équipe]
Documentation : `./Documentation/`
Support : `./manage.sh diagnose` puis partager le résultat
