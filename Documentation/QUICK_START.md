# 🚀 Démarrage Rapide - Airflow AMUE

## Installation en 3 Minutes

### 1️⃣ Cloner et Préparer

```bash
# Cloner le projet
git clone <votre-repo>
cd airflow-amue

# Rendre les scripts exécutables
chmod +x manage.sh
chmod +x scripts/*.sh
```

### 2️⃣ Configurer les Credentials

Modifiez `config/airflow_connections.json`:

```json
{
  "oauth_api": {
    "conn_type": "http",
    "host": "https://api.amue.fr",
    "login": "VOTRE_CLIENT_ID",          ← Changez ici
    "password": "VOTRE_CLIENT_SECRET",    ← Changez ici
    "extra": {
      "token_url": "https://oauth.amue.fr/token",
      "grant_type": "client_credentials"
    }
  }
}
```

### 3️⃣ Lancer le Setup

```bash
./manage.sh setup
```

✅ **C'est tout !** Airflow est configuré et prêt.

---

## 🎯 Utilisation Quotidienne

### Commandes Essentielles

```bash
# Démarrer Airflow
./manage.sh start

# Voir l'état
./manage.sh status

# Voir les logs
./manage.sh logs scheduler

# Arrêter Airflow
./manage.sh stop
```

### Accès à l'Interface

- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

### Déclencher un Import

```bash
# Via CLI
./manage.sh trigger amue_multi_table_import_v2

# Ou via l'interface web
# Aller sur http://localhost:8080
# Cliquer sur le DAG > Bouton "Play"
```

---

## 📋 Commandes Principales

### Gestion des Services

| Commande | Description |
|----------|-------------|
| `./manage.sh start` | Démarre tous les services |
| `./manage.sh stop` | Arrête tous les services |
| `./manage.sh restart` | Redémarre tous les services |
| `./manage.sh status` | Affiche l'état des services |
| `./manage.sh logs [service]` | Affiche les logs |

### Configuration

| Commande | Description |
|----------|-------------|
| `./manage.sh setup` | Installation complète |
| `./manage.sh config` | Reconfigure Airflow |
| `./manage.sh verify` | Vérifie la config |
| `./manage.sh export` | Exporte la config |

### DAGs

| Commande | Description |
|----------|-------------|
| `./manage.sh dags` | Liste les DAGs |
| `./manage.sh trigger <dag_id>` | Déclenche un DAG |
| `./manage.sh pause <dag_id>` | Met en pause |
| `./manage.sh unpause <dag_id>` | Réactive |

### Base de Données

| Commande | Description |
|----------|-------------|
| `./manage.sh db-shell` | Shell PostgreSQL |
| `./manage.sh db-backup` | Sauvegarde la BDD |
| `./manage.sh db-restore <file>` | Restaure une backup |

---

## 🔧 Configuration Avancée

### Ajouter une Table

1. Éditez `config/airflow_variables.json`
2. Ajoutez dans `amue_tables_to_import`:

```json
{
  "name": "MA_TABLE",
  "primary_key": "ID_COLONNE",
  "delta": "DATE_MAJ",
  "last_import": "",
  "finger_print": ""
}
```

3. Reconfigurez:
```bash
./manage.sh config
```

### Modifier les Paramètres

Éditez `config/airflow_variables.json`:

```json
{
  "environment": "production",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_report_recipients": "admin@example.com,team@example.com"
}
```

Puis:
```bash
./manage.sh config
```

---

## 🐛 Résolution de Problèmes

### Les services ne démarrent pas

```bash
# Vérifier les logs
./manage.sh logs

# Redémarrer
./manage.sh stop
./manage.sh start
```

### Le DAG n'apparaît pas

```bash
# Vérifier les erreurs de parsing
./manage.sh shell
airflow dags list-import-errors

# Redémarrer le dag-processor
docker-compose restart airflow-dag-processor
```

### Erreur de connexion API

```bash
# Vérifier les connexions
./manage.sh connections

# Tester la connexion
./manage.sh shell
airflow connections test oauth_api
```

### Réinitialisation Complète

```bash
# Arrêter et nettoyer
./manage.sh stop
docker-compose down -v

# Relancer le setup
./manage.sh setup
```

---

## 📊 Monitoring

### Voir les Logs en Temps Réel

```bash
# Tous les services
./manage.sh logs

# Scheduler uniquement
./manage.sh logs airflow-scheduler

# Worker
./manage.sh logs airflow-apiserver
```

### Dashboard Web

1. Ouvrez http://localhost:8080
2. Allez dans **Graph** pour voir le flux d'exécution
3. **Task Duration** pour les performances
4. **Gantt** pour la timeline

---

## 🔒 Sécurité

### Changer les Mots de Passe

1. Créez un fichier `.env`:
```bash
_AIRFLOW_WWW_USER_USERNAME=mon_user
_AIRFLOW_WWW_USER_PASSWORD=mon_password_fort
```

2. Redémarrez:
```bash
./manage.sh restart
```

### Protéger les Credentials

```bash
# Ne commitez JAMAIS ces fichiers:
# - .env
# - config/airflow_connections.json (avec vraies credentials)

# Vérifiez le .gitignore
cat .gitignore
```

---

## 📚 Documentation Complète

Pour plus d'informations:
- [SETUP_GUIDE.md](SETUP_GUIDE.md) - Guide détaillé
- [REFACTORING_DOCUMENTATION.md](REFACTORING_DOCUMENTATION.md) - Architecture
- [Logs](logs/) - Logs d'exécution

---

## 🆘 Aide

```bash
# Aide du gestionnaire
./manage.sh help

# Version des composants
./manage.sh version

# Shell interactif
./manage.sh shell
```

---

## ✅ Checklist Post-Installation

- [ ] Services démarrés: `./manage.sh status`
- [ ] Interface accessible: http://localhost:8080
- [ ] Login réussi (airflow/airflow)
- [ ] DAG visible dans la liste
- [ ] Connexions configurées: `./manage.sh connections`
- [ ] Variables configurées: `./manage.sh variables`
- [ ] Test d'exécution du DAG
- [ ] Emails de notification fonctionnels

---

## 🎓 Prochaines Étapes

1. **Personnaliser la configuration** selon vos besoins
2. **Configurer le scheduler** pour les exécutions automatiques
3. **Mettre en place les alertes** email/Slack
4. **Monitorer les performances** via les logs
5. **Documenter** les procédures spécifiques à votre équipe

---

**Besoin d'aide?** Consultez les logs avec `./manage.sh logs` ou le guide complet dans `SETUP_GUIDE.md`