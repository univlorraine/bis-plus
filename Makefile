SHELL   := /bin/bash
MANAGE  := ./manage.sh
SERVICE ?=
DAG     ?=
TABLE   ?=
DAYS    ?=

.DEFAULT_GOAL := help

.PHONY: help
help: ## Affiche cette aide
	@printf "\n"
	@printf "  \033[1;36m╔══════════════════════════════════════════════════════╗\033[0m\n"
	@printf "  \033[1;36m║           Airflow AMUE — commandes make              ║\033[0m\n"
	@printf "  \033[1;36m╚══════════════════════════════════════════════════════╝\033[0m\n\n"
	@awk 'BEGIN {FS=":.*##"} \
	  /^##@/ { printf "\n  \033[1;33m%s\033[0m\n", substr($$0,5) } \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  \033[1;32m%-22s\033[0m %s\n", $$1, $$2 }' \
	  $(MAKEFILE_LIST)
	@printf "\n  \033[90mRéférence complète : ./manage.sh help\033[0m\n\n"

##@ Installation

.PHONY: setup
setup: ## Installation interactive complète depuis zéro (inclut Oracle ECC)
	$(MANAGE) setup

.PHONY: setup-bluegreen
setup-bluegreen: ## Initialiser les schémas Blue/Green uniquement
	$(MANAGE) setup-bluegreen

.PHONY: config
config: ## Reconfigurer les variables et connexions Airflow
	$(MANAGE) config

##@ Services Docker

.PHONY: start
start: ## Démarrer tous les services
	$(MANAGE) start

.PHONY: stop
stop: ## Arrêter tous les services
	$(MANAGE) stop

.PHONY: restart
restart: ## Redémarrer tous les services
	$(MANAGE) restart

.PHONY: status
status: ## Statut des conteneurs Docker
	$(MANAGE) status

.PHONY: health
health: ## Santé détaillée de tous les services
	$(MANAGE) health

.PHONY: logs
logs: ## Logs en direct (SERVICE=<nom> pour un service précis)
	$(MANAGE) logs $(SERVICE)

.PHONY: refresh-plugins
refresh-plugins: ## Recharger les plugins (redémarre le scheduler)
	$(MANAGE) refresh-plugins

##@ Gestion des tables (splus_admin.amue_tables)

.PHONY: tables
tables: ## Lister toutes les tables configurées (AMUE + ECC)
	$(MANAGE) list-tables

.PHONY: add-table
add-table: ## Ajouter une ou plusieurs tables (TABLE="T1 T2" ou interactif)
	$(MANAGE) add-table $(TABLE)

.PHONY: remove-table
remove-table: ## Supprimer une table de la configuration (TABLE=<nom> requis)
	@test -n "$(TABLE)" || (printf "Usage: make remove-table TABLE=<nom>\n" && exit 1)
	$(MANAGE) remove-table $(TABLE)

.PHONY: toggle-table
toggle-table: ## Activer/désactiver une table (TABLE=<nom> requis)
	@test -n "$(TABLE)" || (printf "Usage: make toggle-table TABLE=<nom>\n" && exit 1)
	$(MANAGE) toggle-table $(TABLE)

##@ Diagnostic & Réparation

.PHONY: diagnose
diagnose: ## Diagnostic complet du système
	$(MANAGE) diagnose

.PHONY: fix
fix: ## Corriger la configuration (attend l'API)
	$(MANAGE) fix

.PHONY: auto-fix
auto-fix: ## Détection et réparation automatiques
	$(MANAGE) auto-fix

.PHONY: verify
verify: ## Vérifier la configuration courante
	$(MANAGE) verify

.PHONY: test-config
test-config: ## Test rapide de la configuration
	$(MANAGE) test-config

##@ Tests

.PHONY: test
test: ## Lancer la suite pytest
	$(MANAGE) tests

.PHONY: test-cov
test-cov: ## Tests avec rapport de couverture HTML (→ htmlcov/)
	$(MANAGE) tests-cov

.PHONY: test-email
test-email: ## Tester la configuration SMTP/MailHog
	$(MANAGE) test-email

.PHONY: validate
validate: ## Valider la syntaxe des DAGs
	$(MANAGE) validate

.PHONY: lint
lint: ## Linter le code des DAGs
	$(MANAGE) lint

##@ DAGs & Développement

.PHONY: dags
dags: ## Lister tous les DAGs Airflow
	$(MANAGE) dags

.PHONY: trigger
trigger: ## Déclencher un DAG manuellement (DAG=<dag_id> requis)
	@test -n "$(DAG)" || (printf "Usage: make trigger DAG=<dag_id>\n" && exit 1)
	$(MANAGE) trigger $(DAG)

.PHONY: demo
demo: ## Scénarios de démonstration d'erreurs DAG
	./scripts/dev/demo_failures.sh

.PHONY: shell
shell: ## Shell interactif dans le conteneur apiserver
	$(MANAGE) shell

.PHONY: python
python: ## Console Python dans le conteneur apiserver
	$(MANAGE) python

.PHONY: db-shell
db-shell: ## Shell PostgreSQL interactif (postgres-data)
	$(MANAGE) db-shell

##@ Maintenance

.PHONY: clean
clean: ## Supprimer les fichiers temporaires Python (__pycache__, .pyc)
	$(MANAGE) clean

.PHONY: cleanup-logs
cleanup-logs: ## Supprimer les logs anciens (DAYS=30)
	$(MANAGE) cleanup-logs $(DAYS)

.PHONY: cleanup-db
cleanup-db: ## Purger les vieux DAG runs de la DB (DAYS=30)
	$(MANAGE) cleanup-db $(DAYS)

.PHONY: export
export: ## Exporter la configuration Airflow courante
	$(MANAGE) export

.PHONY: reset
reset: ## DANGER : reset complet — détruit tous les volumes
	$(MANAGE) reset
