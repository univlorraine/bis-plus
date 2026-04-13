# Base intermédiaire SifacPlus

Outil d'import automatisé des données financières SIFAC+ et ECC vers une base PostgreSQL, orchestré par Apache Airflow.

Les données sont importées quotidiennement depuis l'API SIFAC+, avec détection des changements de structure, import différentiel, et rollback instantané via une architecture Blue/Green.

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
- [Installation](INSTALL.md) — prérequis, configuration, commandes, résolution de problèmes
