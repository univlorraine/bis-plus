# Base intermédiaire SifacPlus

Pipeline d'intégration de données financières universitaires vers PostgreSQL, orchestré par Apache Airflow.

Deux sources sont prises en charge : l'API **AMUE (SIFAC+)** pour l'import quotidien des données financières, et les bases **Oracle ECC** pour les tables SAP complémentaires.

L'architecture Blue/Green garantit des bascules atomiques et un rollback instantané — les vues publiques `splus.*` pointent toujours vers un schéma cohérent et complet.

## Prérequis

Docker, Docker Compose, 4 Go RAM, ports 8080 / 5432 / 8025 libres.

## Installation

```bash
git clone <votre-repo>
cd sifacplus
chmod +x manage.sh scripts/**/*.sh
./manage.sh setup
```

Le script configure l'environnement, les credentials API AMUE et PostgreSQL, puis initialise la base.


## Documentation

- [Fonctionnement](docs/fonctionnement.md) — cycle d'import, Blue/Green, rollback, notifications
- [Technique](docs/technique.md) — structure du projet, schémas PostgreSQL, principes de conception
- [Opérations](docs/OPERATIONS.md) — scénarios quotidiens, gestion des tables, correction, dépannage
- [Installation](INSTALL.md) — prérequis, configuration, commandes, résolution de problèmes
- [Présentation](PRESENTATION.md) — architecture détaillée des DAGs, composants, variables
- [Mise à jour](docs/UPGRADE.md) — procédure de mise à jour, migrations SQL, rollback de release
- [Déploiement](docs/DEPLOYMENT.md) — guide de déploiement complet, variables, connexions
- [Dépannage](docs/TROUBLESHOOTING.md) — erreurs courantes, diagnostic, escalade
- [Concepts](docs/CONCEPTS.md) — glossaire, principes fondamentaux, architecture Blue/Green
- [API AMUE — tables disponibles](openapi.md) — référence des tables SAP/SIFAC+ avec colonnes Delta
