#!/bin/bash

###############################################################################
# Script de gestion Airflow AMUE
# Centralise toutes les opérations courantes
###############################################################################

set -e
set -o pipefail

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

trap 'log_error "Erreur à la ligne $LINENO — commande : $BASH_COMMAND"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BISPLUS_VERSION=$(cat "$SCRIPT_DIR/VERSION" 2>/dev/null | tr -d '[:space:]')
BISPLUS_VERSION="${BISPLUS_VERSION:-1.0.0}"
export BISPLUS_VERSION
AIRFLOW_IMAGE_NAME="${AIRFLOW_IMAGE_NAME:-bisplus:${BISPLUS_VERSION}}"
export AIRFLOW_IMAGE_NAME

# Détecte docker-compose ou docker compose
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

###############################################################################
# Fonctions
###############################################################################

# Wrapper : exécute une commande airflow dans le container en filtrant les logs
# de démarrage alembic (ils précèdent la config logging d'Airflow)
airflow_exec() {
    $DOCKER_CMD exec -T airflow-apiserver airflow "$@" 2>&1 \
        | { grep -v "\[alembic\.runtime\.plugins\]" || true; }
    return ${PIPESTATUS[0]}
}

show_banner() {
    cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                   Gestionnaire Airflow AMUE                   ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
    echo ""
}

show_help() {
    cat << EOF
Usage: ./manage.sh [COMMAND]

Commandes disponibles:

  GESTION DES SERVICES
    build               Reconstruit l'image Docker (jq + packages pré-installés)
    start               Démarre tous les services (build auto si image absente)
    stop                Arrête tous les services
    restart             Redémarre tous les services
    refresh-plugins     Rafraichi les plugins
    status              Affiche l'état des services
    logs [service]      Affiche les logs (optionnel: nom du service)
    health              Vérifie la santé de tous les services

  CONFIGURATION
    setup               Installation complète initiale (build image + Blue/Green)
    setup-bluegreen     Initialise uniquement les schémas Blue/Green
    config              Reconfigure les variables et connexions
    fix                 Corrige la configuration (avec attente API)
    auto-fix            Correction automatique complète (détecte et corrige)
    verify              Vérifie la configuration actuelle
    export              Exporte la configuration actuelle
    diagnose            Diagnostic complet du système
    test-config         Test rapide de la configuration

  AIRFLOW - DAGs
    dags                Liste tous les DAGs
    trigger [dag_id]    Déclenche un DAG manuellement
    pause [dag_id]      Met en pause un DAG
    unpause [dag_id]    Réactive un DAG
    pause-all           Met en pause tous les DAGs
    unpause-all         Réactive tous les DAGs
    backfill [dag_id]   Relance des exécutions passées

  AIRFLOW - MONITORING
    failed [limit]      Liste les tâches en échec récentes
    task-logs [dag] [task] [run]  Affiche les logs d'une tâche
    validate            Valide la syntaxe des DAGs
    lint                Analyse le code des DAGs

  AIRFLOW - RESSOURCES
    variables           Liste les variables Airflow
    connections         Liste les connexions Airflow
    users               Liste les utilisateurs Airflow
    add-user            Crée un nouvel utilisateur
    delete-user [user]  Supprime un utilisateur
    add-table [t1 t2..] Ajoute une ou plusieurs tables (enable=true)
    load-tables          Ajouter/mettre à jour des tables dans splus_admin.amue_tables (interactif)
    list-tables         Liste les tables configurées avec leur statut
    remove-table <t1..> Supprime une ou plusieurs tables
    toggle-table <t1..> Active/désactive une ou plusieurs tables
    enable-table <t1..> Active une ou plusieurs tables
    disable-table <t1..> Désactive une ou plusieurs tables

  GESTION DES VARIABLES
    var-get <key>       Affiche la valeur d'une variable
    var-set <key> <val> Définit une variable (val optionnel = mode interactif)
    var-delete <key>    Supprime une variable
    var-export [file]   Exporte les variables vers un fichier JSON
    var-import <file>   Importe les variables depuis un fichier JSON

  GESTION DES CONNEXIONS
    conn-test [name]    Teste les connexions (toutes si pas de nom)
    conn-export         Exporte les connexions (sans secrets)
    conn-update <name>  Met à jour une connexion existante

  CONFIGURATION GLOBALE
    config-validate     Valide toute la configuration (.env, variables, connexions)
    config-backup       Sauvegarde complète de la configuration
    config-restore <f>  Restaure depuis une sauvegarde
    api-source [--force] Change la source d'API AMUE (cdv|entrepot) ; --force pour écraser

  BASE DE DONNÉES
    db-shell            Connexion au shell PostgreSQL
    db-backup           Sauvegarde la base de données
    db-restore [file]   Restaure une sauvegarde
    db-migrate          Applique les migrations SQL applicatives en attente

  MISE À JOUR
    update [tag]        Met à jour le projet vers un tag git (via git fetch/checkout)
                        (sans argument : dernier tag ; --resume pour reprendre)
                        Voir docs/UPGRADE.md pour la procédure complète

  MAINTENANCE
    cleanup-logs [days] Supprime les logs > N jours (défaut: 30)
    cleanup-db [days]   Purge les anciennes exécutions (défaut: 30)
    reset               Reset complet (supprime tout et recréé)
    clean               Nettoie les fichiers temporaires Python

  DÉVELOPPEMENT
    test [dag_id]       Test un DAG
    test-email          Test la configuration email
    tests [file]        Lance les tests pytest
    tests-cov           Lance les tests avec couverture
    shell               Shell interactif dans le container
    python              Console Python interactive

  AUTRES
    help                Affiche cette aide
    version             Affiche les versions

Exemples:
  ./manage.sh start
  ./manage.sh health
  ./manage.sh logs airflow-scheduler
  ./manage.sh trigger amue_multi_table_import
  ./manage.sh failed 10
  ./manage.sh task-logs my_dag my_task
  ./manage.sh backfill my_dag 2024-01-01 2024-01-31
  ./manage.sh cleanup-logs 7
  ./manage.sh validate

Interfaces:
  - Airflow UI : http://localhost:8080 (airflow/airflow)
  - MailHog UI : http://localhost:8025 (voir les emails)

EOF
}

###############################################################################
# Build de l'image Docker personnalisée
###############################################################################

# Retourne le nom de l'image (priorité : env AIRFLOW_IMAGE_NAME > BISPLUS_VERSION > défaut)
_get_airflow_image_name() {
    echo "${AIRFLOW_IMAGE_NAME:-bisplus:${BISPLUS_VERSION:-1.0.0}}"
}

# Construit l'image si le Dockerfile est présent. Utilisé lors du setup et du reset.
_build_image() {
    [[ ! -f "Dockerfile" ]] && return 0
    log_info "Construction de l'image Docker (jq + packages pip pré-installés)..."
    if ! $DOCKER_CMD build; then
        log_error "Échec de la construction de l'image Docker"
        exit 1
    fi
    log_success "Image Docker construite"
}

# Construit l'image uniquement si elle est absente localement.
# Utilisé par cmd_start pour couvrir le premier démarrage après un clone.
_build_image_if_missing() {
    [[ ! -f "Dockerfile" ]] && return 0
    local image
    image=$(_get_airflow_image_name)
    if [[ -z "$(docker images -q "$image" 2>/dev/null)" ]]; then
        log_info "Image '$image' absente — construction initiale..."
        _build_image
    fi
}

cmd_start() {
    log_info "Démarrage des services..."
    _build_image_if_missing
    $DOCKER_CMD up -d

    # S'assurer que MailHog est démarré
    chmod +x scripts/install/ensure_mailhog.sh
    ./scripts/install/ensure_mailhog.sh 2>/dev/null || true

    log_success "Services démarrés"
    sleep 5
    cmd_status

    echo ""
    log_info "Interfaces disponibles:"
    echo "  - Airflow UI : http://localhost:8080"
    echo "  - MailHog UI : http://localhost:8025"
}

cmd_stop() {
    log_info "Arrêt des services..."
    $DOCKER_CMD stop
    log_success "Services arrêtés"
}

cmd_restart() {
    log_info "Redémarrage des services..."
    $DOCKER_CMD restart
    log_success "Services redémarrés"
}

cmd_refresh_plugins() {
    log_info "Rechargement des plugins..."
    $DOCKER_CMD restart airflow-scheduler
    log_success "Plugins redémarrés"
}

cmd_status() {
    log_info "État des services:"
    $DOCKER_CMD ps
}

cmd_logs() {
    local service=${1:-}
    if [[ -z "$service" ]]; then
        log_info "Logs de tous les services (Ctrl+C pour quitter):"
        $DOCKER_CMD logs -f --tail=100
    else
        log_info "Logs du service $service (Ctrl+C pour quitter):"
        $DOCKER_CMD logs -f --tail=100 "$service"
    fi
}

cmd_setup() {
    log_info "Lancement du setup complet..."
    # L'image est construite par docker-compose up dans quick_setup.sh,
    # apres generation du .env avec le bon AIRFLOW_IMAGE_NAME
    chmod +x scripts/install/quick_setup.sh
    ./scripts/install/quick_setup.sh

    # Initialisation Blue/Green après le setup principal
    log_info "Initialisation des schémas Blue/Green..."
    cmd_setup_bluegreen
}

cmd_setup_bluegreen() {
    log_info "Configuration de l'architecture Blue/Green..."

    local PG_HOST PG_PORT PG_DB PG_USER PG_PASSWORD
    _load_pg_creds  # exit 1 si connexion introuvable

    log_info "Connexion à PostgreSQL: postgres-data/$PG_DB (user: $PG_USER)"

    # Vérifie que PostgreSQL est accessible (via docker exec — psql n'est pas requis sur le host)
    log_info "Vérification de la connexion..."
    local retries=30
    while [[ $retries -gt 0 ]]; do
        if $DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" > /dev/null 2>&1; then
            break
        fi
        log_warning "PostgreSQL n'est pas prêt. Attente... ($retries)"
        sleep 2
        ((retries-=1))
    done

    if [[ $retries -eq 0 ]]; then
        log_error "PostgreSQL n'est pas accessible après 60 secondes"
        return 1
    fi

    log_success "Connexion PostgreSQL OK"

    # Crée les schémas Blue/Green
    log_info "Création des schémas splus_blue et splus_green..."
    $DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" << EOSQL
-- Création des schémas Blue/Green
CREATE SCHEMA IF NOT EXISTS splus;
CREATE SCHEMA IF NOT EXISTS splus_blue;
CREATE SCHEMA IF NOT EXISTS splus_green;

-- Permissions sur le schéma principal (vues)
GRANT ALL PRIVILEGES ON SCHEMA splus TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus TO $PG_USER;

-- Permissions sur splus_blue
GRANT ALL PRIVILEGES ON SCHEMA splus_blue TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_blue TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_blue TO $PG_USER;

-- Permissions sur splus_green
GRANT ALL PRIVILEGES ON SCHEMA splus_green TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_green TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_green TO $PG_USER;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA splus
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $PG_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_blue
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $PG_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_green
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $PG_USER;

-- Définir le search_path par défaut
ALTER ROLE $PG_USER SET search_path TO splus, splus_blue, splus_green, public;

SELECT 'Blue/Green schemas created successfully' AS status;
EOSQL

    if [[ $? -ne 0 ]]; then
        log_error "Erreur lors de la création des schémas Blue/Green"
        return 1
    fi

    log_success "Schémas Blue/Green créés avec succès"
    log_info "  - splus       : schéma des vues (interface publique)"
    log_info "  - splus_blue  : schéma des tables blue"
    log_info "  - splus_green : schéma des tables green"

    # Crée le schéma admin (état centralisé) — idempotent, au cas où init_db.sql
    # n'aurait pas tourné (volume existant depuis un run précédent)
    log_info "Création du schéma splus_admin (si absent)..."
    $DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" << EOSQL
CREATE SCHEMA IF NOT EXISTS splus_admin;

CREATE TABLE IF NOT EXISTS splus_admin.amue_state (
    id                    INTEGER PRIMARY KEY DEFAULT 1,
    last_finish_timestamp TIMESTAMPTZ,
    last_successful_run   TIMESTAMPTZ,
    last_report_start     TIMESTAMPTZ,
    active_schema         VARCHAR(20),
    last_switch_timestamp TIMESTAMPTZ,
    last_sync_timestamp   TIMESTAMPTZ,
    import_in_progress    BOOLEAN NOT NULL DEFAULT FALSE,
    import_started_at     TIMESTAMPTZ,
    import_correlation_id VARCHAR(255),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);
INSERT INTO splus_admin.amue_state (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS splus_admin.amue_tables (
    table_name      VARCHAR(100) PRIMARY KEY,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    primary_key     TEXT         NOT NULL DEFAULT '',
    delta           TEXT         NOT NULL DEFAULT '',
    fingerprint_api TEXT         NOT NULL DEFAULT '',
    fingerprint_local TEXT        NOT NULL DEFAULT '',
    setup_status    VARCHAR(20)  NOT NULL DEFAULT 'pending',
    ecc_query       TEXT,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS splus_admin.schema_migrations (
    version     VARCHAR(10)  PRIMARY KEY,
    description TEXT         NOT NULL DEFAULT '',
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by  VARCHAR(100) NOT NULL DEFAULT current_user
);

GRANT ALL PRIVILEGES ON SCHEMA splus_admin TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_admin TO $PG_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_admin TO $PG_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_admin
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $PG_USER;

SELECT 'splus_admin schema ready' AS status;
EOSQL

    if [[ $? -ne 0 ]]; then
        log_error "Erreur lors de la création du schéma splus_admin"
        return 1
    fi

    log_success "Schéma splus_admin prêt"

    # Création des vues dans splus pour les tables existantes dans splus_blue
    log_info "Vérification et création des vues dans splus..."

    # Récupère la liste des tables dans splus_blue qui n'ont pas de vue correspondante dans splus
    local tables_without_views
    tables_without_views=$($DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" -t -A << 'EOSQL'
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'splus_blue'
  AND t.table_type = 'BASE TABLE'
  AND NOT EXISTS (
      SELECT 1
      FROM information_schema.views v
      WHERE v.table_schema = 'splus'
        AND v.table_name = t.table_name
  )
ORDER BY t.table_name;
EOSQL
)

    if [[ -z "$tables_without_views" ]]; then
        log_info "Toutes les vues sont déjà créées (ou aucune table dans splus_blue)"
    else
        local view_count=0
        while IFS= read -r table_name; do
            [[ -z "$table_name" ]] && continue

            if [[ ! "$table_name" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
                log_warning "  Nom de table non sécurisé ignoré : '$table_name'"
                continue
            fi

            log_info "  Création de la vue splus.$table_name -> splus_blue.$table_name"
            $DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" -q << EOSQL
CREATE OR REPLACE VIEW splus.$table_name AS SELECT * FROM splus_blue.$table_name;
GRANT SELECT ON splus.$table_name TO $PG_USER;
EOSQL
            ((view_count+=1))
        done <<< "$tables_without_views"

        log_success "$view_count vue(s) créée(s) dans le schéma splus"
    fi
}

# Applique les migrations SQL applicatives en attente (scripts/sql/migrations/NNNN_*.sql)
# Idempotent : peut être rejouée sans risque (chaque migration doit l'être elle-même,
# voir scripts/sql/migrations/README.md). Suit l'avancement dans splus_admin.schema_migrations.
cmd_db_migrate() {
    _load_pg_creds

    local migrations_dir="scripts/sql/migrations"
    if [[ ! -d "$migrations_dir" ]]; then
        log_warning "Aucun répertoire de migrations ($migrations_dir) — rien à faire"
        return 0
    fi

    local applied
    applied=$(PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data \
        psql -U "$PG_USER" -d "$PG_DB" -t -A \
        -c "SELECT version FROM splus_admin.schema_migrations ORDER BY version;" 2>/dev/null)

    local pending=()
    local file version description
    for file in "$migrations_dir"/[0-9][0-9][0-9][0-9]_*.sql; do
        [[ -e "$file" ]] || continue
        version=$(basename "$file" | cut -d'_' -f1)
        if ! grep -qx "$version" <<< "$applied"; then
            pending+=("$file")
        fi
    done

    if [[ ${#pending[@]} -eq 0 ]]; then
        log_info "Aucune migration en attente"
        return 0
    fi

    log_info "${#pending[@]} migration(s) en attente :"
    for file in "${pending[@]}"; do
        log_info "  - $(basename "$file")"
    done

    for file in "${pending[@]}"; do
        version=$(basename "$file" | cut -d'_' -f1)
        description=$(basename "$file" .sql | cut -d'_' -f2- | tr '_' ' ')

        log_info "Application de la migration $version ($description)..."
        if ! PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data \
            psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -q < "$file"; then
            log_error "Échec de la migration $version ($(basename "$file"))"
            log_error "Le fichier est censé être idempotent : corrigez-le puis relancez './manage.sh db-migrate'"
            return 1
        fi

        PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data \
            psql -U "$PG_USER" -d "$PG_DB" -q -c \
            "INSERT INTO splus_admin.schema_migrations (version, description) VALUES ('$version', '$description') ON CONFLICT (version) DO NOTHING;"

        log_success "Migration $version appliquée"
    done

    log_success "${#pending[@]} migration(s) appliquée(s)"
}

cmd_build() {
    if [[ ! -f "Dockerfile" ]]; then
        log_error "Dockerfile absent — rien à construire"
        exit 1
    fi
    log_info "Construction forcée de l'image Docker..."
    if ! $DOCKER_CMD build "${@}"; then
        log_error "Échec de la construction"
        exit 1
    fi
    log_success "Image reconstruite. Relancez 'manage.sh start' pour l'utiliser."
}

cmd_config() {
    log_info "Reconfiguration d'Airflow..."
    chmod +x scripts/install/setup_airflow_config.sh
    ./scripts/install/setup_airflow_config.sh --external
}

cmd_fix() {
    log_info "Correction de la configuration (avec attente API)..."
    chmod +x scripts/manage/fix_config.sh
    ./scripts/manage/fix_config.sh
}

cmd_auto_fix() {
    log_info "Correction automatique complète..."
    chmod +x scripts/manage/auto_fix.sh
    ./scripts/manage/auto_fix.sh
}

cmd_verify() {
    log_info "Vérification de la configuration..."
    chmod +x scripts/install/setup_airflow_config.sh
    ./scripts/install/setup_airflow_config.sh --verify
}

cmd_diagnose() {
    log_info "Diagnostic complet du système..."
    chmod +x scripts/manage/diagnose.sh
    ./scripts/manage/diagnose.sh
}

cmd_test_config() {
    log_info "Test rapide de la configuration..."
    chmod +x scripts/manage/quick_fix.sh
    ./scripts/manage/quick_fix.sh
}

cmd_export() {
    log_info "Export de la configuration..."
    chmod +x scripts/install/setup_airflow_config.sh
    ./scripts/install/setup_airflow_config.sh --export
}

cmd_dags() {
    log_info "Liste des DAGs:"
    airflow_exec dags list
}

cmd_trigger() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh trigger <dag_id>"
        log_info "DAGs disponibles:"
        cmd_dags
        exit 1
    fi

    log_info "Déclenchement du DAG $dag_id..."
    airflow_exec dags trigger "$dag_id"
    log_success "DAG déclenché"
}

cmd_pause() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh pause <dag_id>"
        exit 1
    fi

    log_info "Mise en pause du DAG $dag_id..."
    airflow_exec dags pause "$dag_id"
    log_success "DAG mis en pause"
}

cmd_unpause() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh unpause <dag_id>"
        exit 1
    fi

    log_info "Réactivation du DAG $dag_id..."
    airflow_exec dags unpause "$dag_id"
    log_success "DAG réactivé"
}

cmd_variables() {
    log_info "Variables Airflow:"
    airflow_exec variables list
}

cmd_connections() {
    log_info "Connexions Airflow:"
    airflow_exec connections list
}

###############################################################################
# Gestion des variables Airflow
###############################################################################

cmd_var_get() {
    local key=${1:-}
    if [[ -z "$key" ]]; then
        log_error "Usage: ./manage.sh var-get <key>"
        echo ""
        log_info "Variables disponibles:"
        cmd_variables
        exit 1
    fi

    log_info "Variable '$key':"
    local value
    value=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get "$key" 2>/dev/null)

    if [[ $? -eq 0 ]]; then
        # Affiche formaté si c'est du JSON
        if command -v jq &> /dev/null && echo "$value" | jq . &> /dev/null; then
            echo "$value" | jq .
        else
            echo "$value"
        fi
    else
        log_error "Variable '$key' non trouvée"
        exit 1
    fi
}

cmd_var_set() {
    local key=${1:-}
    local value=${2:-}

    if [[ -z "$key" ]]; then
        log_error "Usage: ./manage.sh var-set <key> <value>"
        log_info "Pour les valeurs JSON complexes, utilisez des guillemets simples"
        log_info "Exemple: ./manage.sh var-set my_config '{\"key\": \"value\"}'"
        exit 1
    fi

    if [[ -z "$value" ]]; then
        # Mode interactif pour les valeurs longues
        log_info "Entrez la valeur (terminez par une ligne vide):"
        value=""
        while IFS= read -r line </dev/tty; do
            [[ -z "$line" ]] && break
            value+="$line"
        done
    fi

    log_info "Définition de la variable '$key'..."
    airflow_exec variables set "$key" "$value"
    log_success "Variable '$key' définie"
}

cmd_var_delete() {
    local key=${1:-}
    if [[ -z "$key" ]]; then
        log_error "Usage: ./manage.sh var-delete <key>"
        echo ""
        log_info "Variables disponibles:"
        cmd_variables
        exit 1
    fi

    log_warning "Suppression de la variable '$key'..."

    echo -n "Confirmer la suppression ? (o/N) : " >&2
    read -r CONFIRM </dev/tty
    [[ ! "$CONFIRM" =~ ^[oOyY]$ ]] && { log_info "Annulé"; exit 0; }

    airflow_exec variables delete "$key"
    log_success "Variable '$key' supprimée"
}

cmd_var_export() {
    local output_file=${1:-}
    local export_dir="config/exports"
    mkdir -p "$export_dir"

    if [[ -z "$output_file" ]]; then
        local timestamp
        timestamp=$(date +%Y%m%d_%H%M%S)
        output_file="${export_dir}/variables_${timestamp}.json"
    fi

    log_info "Export des variables Airflow..."

    # Export toutes les variables au format JSON
    $DOCKER_CMD exec -T airflow-apiserver airflow variables export - > "$output_file"

    if [[ -s "$output_file" ]]; then
        # Formate le JSON si jq est disponible
        if command -v jq &> /dev/null; then
            local temp_file
            temp_file=$(mktemp)
            jq '.' "$output_file" > "$temp_file" && mv "$temp_file" "$output_file"
        fi
        log_success "Variables exportées vers: $output_file"
        log_info "Nombre de variables: $(jq 'length' "$output_file" 2>/dev/null || echo "N/A")"
    else
        log_error "Échec de l'export"
        rm -f "$output_file"
        exit 1
    fi
}

cmd_var_import() {
    local input_file=${1:-}

    if [[ -z "$input_file" ]] || [[ ! -f "$input_file" ]]; then
        log_error "Usage: ./manage.sh var-import <fichier.json>"
        echo ""
        log_info "Exports disponibles:"
        ls -lh config/exports/variables_*.json 2>/dev/null || log_warning "Aucun export trouvé"
        exit 1
    fi

    log_warning "Import des variables depuis: $input_file"
    log_warning "Les variables existantes avec les mêmes clés seront écrasées!"

    echo -n "Continuer ? (o/N) : " >&2
    read -r CONFIRM </dev/tty
    [[ ! "$CONFIRM" =~ ^[oOyY]$ ]] && { log_info "Annulé"; exit 0; }

    log_info "Import en cours..."
    airflow_exec variables import - < "$input_file"
    log_success "Variables importées"

    echo ""
    cmd_variables
}

###############################################################################
# Gestion des connexions Airflow
###############################################################################

cmd_conn_test() {
    local conn_id=${1:-}

    if [[ -z "$conn_id" ]]; then
        log_info "Test de toutes les connexions configurées..."
        echo ""

        # Test oauth_api (HTTP)
        log_info "=== Test oauth_api (API AMUE) ==="
        if $DOCKER_CMD exec -T airflow-apiserver python -c "
from airflow.sdk import Connection
from airflow.hooks.base import BaseHook
try:
    conn = BaseHook.get_connection('oauth_api')
    print(f'  Host: {conn.host}')
    print(f'  Port: {conn.port or \"default\"}')
    print(f'  Schema: {conn.schema or \"N/A\"}')
    print('  Status: Configuration OK')
except Exception as e:
    print(f'  Erreur: {e}')
" 2>/dev/null; then
            log_success "oauth_api: accessible"
        else
            log_error "oauth_api: erreur"
        fi

        echo ""

        # Test postgres_data (PostgreSQL)
        log_info "=== Test postgres_data (PostgreSQL) ==="
        if $DOCKER_CMD exec -T airflow-apiserver python -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
try:
    hook = PostgresHook(postgres_conn_id='postgres_data')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT version();')
    version = cursor.fetchone()[0]
    print(f'  Version: {version[:50]}...')
    cursor.close()
    conn.close()
    print('  Status: Connexion réussie')
except Exception as e:
    print(f'  Erreur: {e}')
" 2>/dev/null; then
            log_success "postgres_data: connecté"
        else
            log_error "postgres_data: erreur de connexion"
        fi

        return
    fi

    # Test d'une connexion spécifique
    log_info "Test de la connexion '$conn_id'..."

    local conn_json
    conn_json=$($DOCKER_CMD exec -T airflow-apiserver airflow connections get "$conn_id" --output json 2>/dev/null)
    if [[ $? -ne 0 ]] || [[ -z "$conn_json" ]]; then
        log_error "Connexion '$conn_id' introuvable ou erreur Airflow"
        return 1
    fi
    # $conn_id passé via argument Airflow CLI (pas interpolé dans du code Python)
    # conn_json lu depuis stdin uniquement — pas d'injection possible
    echo "$conn_json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
r = data[0] if isinstance(data, list) else data
print('Type:', r.get('conn_type', 'N/A'))
print('Host:', r.get('host', 'N/A'))
print('Port:', r.get('port') or 'default')
print('Schema:', r.get('schema') or 'N/A')
print('Login:', r.get('login') or 'N/A')
print('Status: Configuration OK')
"
}

cmd_conn_export() {
    local export_dir="config/exports"
    mkdir -p "$export_dir"
    local timestamp output_file
    timestamp=$(date +%Y%m%d_%H%M%S)
    output_file="${export_dir}/connections_${timestamp}.json"

    log_info "Export des connexions Airflow (sans secrets)..."

    # Export via CLI (compatible Airflow 3.x), masquage password/extra via jq
    local _raw
    _raw=$($DOCKER_CMD exec -T airflow-apiserver \
        airflow connections export - --file-format json 2>/dev/null)

    if [[ -z "$_raw" ]]; then
        log_error "Échec de l'export — airflow connections export a retourné vide"
        exit 1
    fi

    # Le format peut être une liste [] ou un objet {} selon la version d'Airflow
    echo "$_raw" | jq '
        if type == "array" then
            [ .[] | .password = (if .password then "***MASKED***" else null end)
                  | .extra    = (if .extra    then "***MASKED***" else null end) ]
        else
            with_entries(
                .value.password = (if .value.password then "***MASKED***" else null end) |
                .value.extra    = (if .value.extra    then "***MASKED***" else null end)
            )
        end' > "$output_file" 2>/dev/null

    if [[ -s "$output_file" ]]; then
        log_success "Connexions exportées vers: $output_file"
        log_warning "Note: Les mots de passe et extras sont masqués"
        echo ""
        log_info "Connexions exportées:"
        jq -r 'if type == "array" then .[].conn_id else keys[] end' \
            "$output_file" 2>/dev/null | while read -r conn; do
            echo "  - $conn"
        done
    else
        log_error "Échec du masquage"
        rm -f "$output_file"
        exit 1
    fi
}

cmd_conn_update() {
    local conn_id=${1:-}

    if [[ -z "$conn_id" ]]; then
        log_error "Usage: ./manage.sh conn-update <conn_id>"
        echo ""
        log_info "Connexions disponibles:"
        cmd_connections
        exit 1
    fi

    log_info "Mise à jour de la connexion '$conn_id'"
    echo ""

    # Récupère les infos actuelles
    local current_info
    current_info=$($DOCKER_CMD exec -T airflow-apiserver python -c "
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('$conn_id')
print(f'{conn.conn_type}|{conn.host or \"\"}|{conn.port or \"\"}|{conn.schema or \"\"}|{conn.login or \"\"}')
" 2>/dev/null)

    if [[ -z "$current_info" ]]; then
        log_error "Connexion '$conn_id' non trouvée"
        exit 1
    fi

    IFS='|' read -r curr_type curr_host curr_port curr_schema curr_login <<< "$current_info"

    echo "Valeurs actuelles (appuyez sur Entrée pour garder):" >&2
    echo "" >&2

    echo -n "Type [$curr_type] : " >&2
    read -r NEW_TYPE </dev/tty
    [[ -z "$NEW_TYPE" ]] && NEW_TYPE="$curr_type"

    echo -n "Host [$curr_host] : " >&2
    read -r NEW_HOST </dev/tty
    [[ -z "$NEW_HOST" ]] && NEW_HOST="$curr_host"

    echo -n "Port [$curr_port] : " >&2
    read -r NEW_PORT </dev/tty
    [[ -z "$NEW_PORT" ]] && NEW_PORT="$curr_port"

    echo -n "Schema [$curr_schema] : " >&2
    read -r NEW_SCHEMA </dev/tty
    [[ -z "$NEW_SCHEMA" ]] && NEW_SCHEMA="$curr_schema"

    echo -n "Login [$curr_login] : " >&2
    read -r NEW_LOGIN </dev/tty
    [[ -z "$NEW_LOGIN" ]] && NEW_LOGIN="$curr_login"

    echo -n "Password (vide = inchangé) : " >&2
    read -rs NEW_PASSWORD </dev/tty
    echo "" >&2

    log_info "Mise à jour de la connexion..."

    # Suppression puis recréation avec un tableau bash (évite les problèmes d'apostrophes)
    $DOCKER_CMD exec -T airflow-apiserver airflow connections delete "$conn_id" 2>/dev/null || true

    local add_cmd=(airflow connections add "$conn_id" --conn-type "$NEW_TYPE")
    [[ -n "$NEW_HOST"     ]] && add_cmd+=(--conn-host     "$NEW_HOST")
    [[ -n "$NEW_PORT"     ]] && add_cmd+=(--conn-port     "$NEW_PORT")
    [[ -n "$NEW_SCHEMA"   ]] && add_cmd+=(--conn-schema   "$NEW_SCHEMA")
    [[ -n "$NEW_LOGIN"    ]] && add_cmd+=(--conn-login    "$NEW_LOGIN")
    [[ -n "$NEW_PASSWORD" ]] && add_cmd+=(--conn-password "$NEW_PASSWORD")

    $DOCKER_CMD exec -T airflow-apiserver "${add_cmd[@]}"

    log_success "Connexion '$conn_id' mise à jour"
}

###############################################################################
# Configuration globale
###############################################################################

cmd_config_validate() {
    log_info "Validation de la configuration complète..."
    echo ""
    local errors=0

    # 1. Vérification du fichier .env
    log_info "=== Fichier .env ==="
    if [[ -f ".env" ]]; then
        # Variables obligatoires — leur absence bloque le démarrage
        local required_vars=(
            "AIRFLOW__CORE__FERNET_KEY"
            "AIRFLOW__API__SECRET_KEY"
            "AIRFLOW_IMAGE_NAME"
            "AIRFLOW_ENV"
        )
        # Variables CAS — optionnelles : fallback AUTH_DB avec warning si absentes
        local cas_vars=(
            "CAS_SERVER_URL"
        )

        for var in "${required_vars[@]}"; do
            local val
            val=$(grep "^${var}=" .env 2>/dev/null | cut -d'=' -f2-)
            if [[ -z "$val" ]]; then
                echo "  ✗ $var (manquant ou vide)"
                ((errors+=1))
            elif echo "$val" | grep -qE '<À_COMPLÉTER>|REMPLACER_PAR'; then
                echo "  ✗ $var (placeholder non remplacé)"
                ((errors+=1))
            else
                echo "  ✓ $var"
            fi
        done

        local cas_missing=0
        for var in "${cas_vars[@]}"; do
            local val
            val=$(grep "^${var}=" .env 2>/dev/null | cut -d'=' -f2-)
            if [[ -z "$val" ]] || echo "$val" | grep -qE '<À_COMPLÉTER>|REMPLACER_PAR'; then
                echo "  ⚠ $var (non configuré — fallback AUTH_DB actif)"
                ((cas_missing+=1))
            else
                echo "  ✓ $var"
            fi
        done
        if [[ $cas_missing -gt 0 ]]; then
            log_warning "CAS non configuré : Airflow utilise l'authentification locale (dev uniquement)"
        else
            # CAS configuré — afficher les variables optionnelles pour information
            local cas_optional_vars=(CAS_VERSION CAS_DEFAULT_ROLE CAS_SERVICE_URL)
            local cas_optional_defaults=("2" "Viewer" "(auto)")
            echo ""
            log_info "  Variables CAS optionnelles :"
            for i in "${!cas_optional_vars[@]}"; do
                local ovar="${cas_optional_vars[$i]}"
                local oval
                oval=$(grep "^${ovar}=" .env 2>/dev/null | cut -d'=' -f2-)
                local display="${oval:-${cas_optional_defaults[$i]}}"
                echo "    ${ovar} = ${display}"
            done
        fi
    else
        log_error ".env non trouvé"
        ((errors+=1))
    fi

    echo ""

    # 2. Comparaison avec .env.example
    log_info "=== Comparaison .env vs .env.example ==="
    if [[ -f ".env" ]] && [[ -f ".env.example" ]]; then
        local missing=0
        while IFS= read -r line; do
            [[ "$line" =~ ^#.*$ ]] && continue
            [[ -z "$line" ]] && continue
            local var_name
            var_name=$(echo "$line" | cut -d'=' -f1)
            if ! grep -q "^${var_name}=" .env; then
                echo "  ✗ $var_name (dans .env.example mais pas dans .env)"
                ((missing+=1))
            fi
        done < .env.example

        if [[ $missing -eq 0 ]]; then
            log_success "Toutes les variables de .env.example sont présentes"
        else
            ((errors+=missing))
        fi
    else
        log_warning ".env.example non trouvé, comparaison ignorée"
    fi

    echo ""

    # 3. Vérification des variables Airflow
    log_info "=== Variables Airflow ==="
    local airflow_vars=(
        "universite"
        "api_endpoint_admin"
        "api_endpoint_table"
        "amue_api_source"
        "api_endpoint_entrepot"
        "amue_import_batch_size"
        "amue_report_recipients"
        "ecc_report_recipients"
        "smtp_host"
        "smtp_port"
        "smtp_mail_from"
    )

    # Fetch une seule fois pour eviter N docker exec separes
    local _vars_json
    _vars_json=$($DOCKER_CMD exec -T airflow-apiserver airflow variables list --output json 2>/dev/null || echo "[]")

    for var in "${airflow_vars[@]}"; do
        local _val
        _val=$(printf '%s' "$_vars_json" | jq -r --arg v "$var" '.[] | select(.key == $v) | .val // ""' 2>/dev/null)
        if [[ -z "$_val" ]]; then
            echo "  ✗ $var (non définie)"
            ((errors+=1))
        elif echo "$_val" | grep -qE 'VOIR_ENV|<À_COMPLÉTER>|REMPLACER_PAR'; then
            echo "  ✗ $var (placeholder non remplacé : $_val)"
            ((errors+=1))
        else
            echo "  ✓ $var"
        fi
    done

    echo ""

    # 4. Vérification des connexions Airflow
    log_info "=== Connexions Airflow ==="
    # Fetch une seule fois pour eviter 2 docker exec separes
    local _conns_json
    _conns_json=$($DOCKER_CMD exec -T airflow-apiserver airflow connections list --output json 2>/dev/null || echo "[]")

    for conn_id in oauth_api postgres_data; do
        local login
        login=$(printf '%s' "$_conns_json" | jq -r --arg c "$conn_id" '.[] | select(.conn_id == $c) | .login // ""')
        if [[ -n "$login" ]]; then
            echo "  ✓ $conn_id (login: $login)"
        else
            echo "  ✗ $conn_id (connexion absente ou sans credentials)"
            ((errors+=1))
        fi
    done

    echo ""

    # 5. Vérification des fichiers de configuration
    log_info "=== Fichiers de configuration ==="
    local config_files=(
        "config/airflow_variables.json"
        "docker-compose.yml"
    )

    for file in "${config_files[@]}"; do
        if [[ -f "$file" ]]; then
            echo "  ✓ $file"
        else
            echo "  ✗ $file (manquant)"
            ((errors+=1))
        fi
    done

    echo ""

    # Résultat final
    if [[ $errors -eq 0 ]]; then
        log_success "Configuration valide (0 erreur)"
    else
        log_error "Configuration invalide ($errors erreur(s))"
        exit 1
    fi
}

cmd_config_backup() {
    local backup_dir="backups/config"
    mkdir -p "$backup_dir"
    local timestamp backup_name backup_path
    timestamp=$(date +%Y%m%d_%H%M%S)
    backup_name="config_backup_${timestamp}"
    backup_path="${backup_dir}/${backup_name}"

    log_info "Sauvegarde de la configuration complète..."
    mkdir -p "$backup_path"

    # Sauvegarde des fichiers locaux
    log_info "Sauvegarde des fichiers locaux..."
    cp -r config/*.json "$backup_path/" 2>/dev/null || true
    cp .env "$backup_path/.env" 2>/dev/null || true
    log_warning "  Le fichier .env (clé Fernet) est inclus — ne partagez pas cette archive"

    # Export des variables Airflow
    log_info "Export des variables Airflow..."
    $DOCKER_CMD exec -T airflow-apiserver airflow variables export - > "$backup_path/airflow_variables_export.json" 2>/dev/null || true

    # Export des connexions (format URI)
    log_info "Export des connexions Airflow..."
    $DOCKER_CMD exec -T airflow-apiserver airflow connections export - --file-format json > "$backup_path/airflow_connections_export.json" 2>/dev/null || true

    # Créer une archive
    log_info "Création de l'archive..."
    tar -czf "${backup_path}.tar.gz" -C "$backup_dir" "$backup_name"
    rm -rf "$backup_path"

    log_success "Sauvegarde créée: ${backup_path}.tar.gz"

    # Liste les sauvegardes existantes
    echo ""
    log_info "Sauvegardes disponibles:"
    ls -lh "$backup_dir"/*.tar.gz 2>/dev/null | tail -5
}

cmd_config_restore() {
    local backup_file=${1:-}

    if [[ -z "$backup_file" ]] || [[ ! -f "$backup_file" ]]; then
        log_error "Usage: ./manage.sh config-restore <fichier_backup.tar.gz>"
        echo ""
        log_info "Sauvegardes disponibles:"
        ls -lh backups/config/*.tar.gz 2>/dev/null || log_warning "Aucune sauvegarde trouvée"
        exit 1
    fi

    log_warning "ATTENTION: Cette opération va restaurer la configuration!"
    log_warning "Les configurations actuelles seront écrasées."
    echo ""

    echo -n "Continuer ? (o/N) : " >&2
    read -r CONFIRM </dev/tty
    [[ ! "$CONFIRM" =~ ^[oOyY]$ ]] && { log_info "Annulé"; exit 0; }

    local temp_dir backup_dir
    temp_dir=$(mktemp -d)
    log_info "Extraction de la sauvegarde..."
    tar -xzf "$backup_file" -C "$temp_dir"

    # Trouve le répertoire extrait (robuste même si le tar contient plusieurs items)
    backup_dir=$(find "$temp_dir" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)
    if [[ -z "$backup_dir" ]]; then
        log_error "Archive invalide — aucun répertoire trouvé"
        rm -rf "$temp_dir"
        exit 1
    fi

    # Restauration des fichiers de config
    if [[ -f "$backup_dir/airflow_variables.json" ]]; then
        log_info "Restauration de airflow_variables.json..."
        cp "$backup_dir/airflow_variables.json" config/
    fi

    if [[ -f "$backup_dir/airflow_connections.json" ]]; then
        log_info "Restauration de airflow_connections.json..."
        cp "$backup_dir/airflow_connections.json" config/
    fi

    # Restauration du .env (avec confirmation)
    if [[ -f "$backup_dir/.env" ]]; then
        echo -n "Restaurer aussi le fichier .env ? (o/N) : " >&2
        read -r CONFIRM_ENV </dev/tty
        if [[ "$CONFIRM_ENV" =~ ^[oOyY]$ ]]; then
            cp "$backup_dir/.env" .env
            log_info ".env restauré"
        fi
    fi

    # Import des variables Airflow
    if [[ -f "$backup_dir/airflow_variables_export.json" ]]; then
        log_info "Import des variables Airflow..."
        $DOCKER_CMD exec -T airflow-apiserver airflow variables import - < "$backup_dir/airflow_variables_export.json" 2>/dev/null || true
    fi

    # Import des connexions Airflow
    if [[ -f "$backup_dir/airflow_connections_export.json" ]]; then
        log_info "Import des connexions Airflow..."
        $DOCKER_CMD exec -T airflow-apiserver airflow connections import "$backup_dir/airflow_connections_export.json" 2>/dev/null || true
    fi

    # Nettoyage
    rm -rf "$temp_dir"

    log_success "Configuration restaurée"
    echo ""
    log_info "Redémarrez les services pour appliquer les changements: ./manage.sh restart"
}

cmd_users() {
    log_info "Utilisateurs Airflow:"
    airflow_exec users list
}

cmd_add_user() {
    log_info "Création d'un nouvel utilisateur"
    echo ""

    echo -n "Username : " >&2
    read -r USERNAME </dev/tty
    [[ -z "$USERNAME" ]] && { log_error "Username obligatoire"; exit 1; }

    echo -n "Email : " >&2
    read -r EMAIL </dev/tty
    [[ -z "$EMAIL" ]] && { log_error "Email obligatoire"; exit 1; }

    echo -n "Prénom : " >&2
    read -r FIRSTNAME </dev/tty
    [[ -z "$FIRSTNAME" ]] && FIRSTNAME="$USERNAME"

    echo -n "Nom : " >&2
    read -r LASTNAME </dev/tty
    [[ -z "$LASTNAME" ]] && LASTNAME="User"

    echo ""
    echo "Rôles disponibles: Admin, Op, User, Viewer, Public" >&2
    echo -n "Rôle [Viewer] : " >&2
    read -r ROLE </dev/tty
    [[ -z "$ROLE" ]] && ROLE="Viewer"

    echo -n "Mot de passe : " >&2
    read -rs PASSWORD </dev/tty
    echo "" >&2
    [[ -z "$PASSWORD" ]] && { log_error "Mot de passe obligatoire"; exit 1; }

    log_info "Création de l'utilisateur $USERNAME avec le rôle $ROLE..."

    airflow_exec users create \
        --username "$USERNAME" \
        --email "$EMAIL" \
        --firstname "$FIRSTNAME" \
        --lastname "$LASTNAME" \
        --role "$ROLE" \
        --password "$PASSWORD"

    log_success "Utilisateur $USERNAME créé"
}

cmd_delete_user() {
    local username=${1:-}
    if [[ -z "$username" ]]; then
        log_error "Usage: ./manage.sh delete-user <username>"
        echo ""
        cmd_users
        exit 1
    fi

    log_warning "Suppression de l'utilisateur $username"
    airflow_exec users delete --username "$username"
    log_success "Utilisateur $username supprimé"
}

###############################################################################
# Helper : récupère un champ d'une connexion Airflow via docker exec
###############################################################################

# Usage: _get_airflow_conn <conn_id> <field>
# Champs disponibles: host, port, schema, login, password, conn_type
_get_airflow_conn() {
    local conn_id=$1 field=$2
    # $field passé en sys.argv[1] — jamais interpolé dans le code Python
    $DOCKER_CMD exec -T airflow-apiserver airflow connections get "$conn_id" \
        --output json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); r=d[0] if isinstance(d,list) else d; print(r.get(sys.argv[1]) or '')" "$field"
}

###############################################################################
# Helper : chargement des credentials PostgreSQL depuis Airflow DB
###############################################################################

_load_pg_creds() {
    local _json _parsed
    _json=$($DOCKER_CMD exec -T airflow-apiserver airflow connections get postgres_data \
        --output json 2>/dev/null)

    # Parse tous les champs en un seul appel Python (séparateur \x1f)
    _parsed=$(echo "$_json" | python3 -c "
import json, sys
d = json.load(sys.stdin)
r = d[0] if isinstance(d, list) else d
print('\x1f'.join([
    r.get('host') or '',
    str(r.get('port') or ''),
    r.get('schema') or '',
    r.get('login') or '',
    r.get('password') or '',
]))" 2>/dev/null)

    IFS=$'\x1f' read -r PG_HOST PG_PORT PG_DB PG_USER PG_PASSWORD <<< "$_parsed"

    PG_PORT=${PG_PORT:-5432}
    if [[ "$PG_HOST" == "postgres-data" ]]; then
        PG_HOST="localhost"
        PG_PORT="${PG_DATA_PORT:-5433}"
    fi

    if [[ -z "$PG_HOST" || -z "$PG_DB" || -z "$PG_USER" ]]; then
        log_error "Connexion postgres_data introuvable dans Airflow DB"
        exit 1
    fi

    if [[ ! "$PG_USER" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
        log_error "PG_USER contient des caractères non autorisés : '$PG_USER'"
        exit 1
    fi

    if [[ -z "$PG_PASSWORD" ]]; then
        echo -n "Mot de passe PostgreSQL ($PG_USER@$PG_HOST/$PG_DB) : " >&2
        read -rs PG_PASSWORD </dev/tty
        echo "" >&2
    fi
}

_pg_exec() {
    PG_HOST="$PG_HOST" PG_PORT="$PG_PORT" PG_DB="$PG_DB" \
    PG_USER="$PG_USER" PG_PASSWORD="$PG_PASSWORD" \
        python3 scripts/lib/pg_client.py "$@"
}

_pg_escape() {
    # Échappe une valeur pour un littéral SQL PostgreSQL ('' pour les apostrophes)
    printf "%s" "${1//\'/\'\'}"
}

###############################################################################
# Gestion des tables (splus_admin.amue_tables)
###############################################################################

cmd_list_tables() {
    _load_pg_creds
    log_info "Tables configurées dans splus_admin.amue_tables:"
    echo ""
    _pg_exec -c "
        SELECT
            CASE WHEN enabled THEN ' ✓' ELSE ' ✗' END AS \"état\",
            table_name AS table,
            CASE WHEN ecc_query IS NOT NULL AND ecc_query != '' THEN 'ECC ' ELSE 'AMUE' END AS type,
            COALESCE(NULLIF(delta,''), '-') AS delta,
            setup_status AS setup
        FROM splus_admin.amue_tables
        ORDER BY type DESC, table_name;
    "
    echo ""
    echo "  ✓ = activée  ✗ = désactivée  |  type ECC = requête Oracle"
    echo ""
}

cmd_add_table() {
    local tables=("$@")
    _load_pg_creds

    if [[ ${#tables[@]} -eq 0 ]]; then
        # Mode interactif
        log_info "Ajout de table — Mode interactif (ligne vide pour terminer)"
        echo ""
        local added=0
        while true; do
            echo -n "Nom de la table (vide pour terminer) : " >&2
            read -r TABLE_NAME </dev/tty
            [[ -z "$TABLE_NAME" ]] && break
            TABLE_NAME="${TABLE_NAME^^}"

            echo -n "Colonne delta pour import différentiel (vide si aucun) : " >&2
            read -r TABLE_DELTA </dev/tty

            echo -n "Table ECC avec requête Oracle ? [y/N] : " >&2
            read -r IS_ECC </dev/tty

            local ECC_SQL="NULL"
            if [[ "${IS_ECC,,}" == "y" ]]; then
                log_info "Entrez la requête Oracle ECC (terminez avec une ligne contenant uniquement 'EOF') :"
                local ECC_LINES=""
                while IFS= read -r line </dev/tty; do
                    [[ "$line" == "EOF" ]] && break
                    ECC_LINES+="$line"$'\n'
                done
                # Échappe les apostrophes pour psql
                ECC_SQL="'$(echo "$ECC_LINES" | sed "s/'/''/g")'"
            fi

            local TABLE_NAME_ESC TABLE_DELTA_ESC
            TABLE_NAME_ESC=$(_pg_escape "$TABLE_NAME")
            TABLE_DELTA_ESC=$(_pg_escape "$TABLE_DELTA")
            RESULT=$(_pg_exec -At -c "
                INSERT INTO splus_admin.amue_tables (table_name, enabled, delta, ecc_query, setup_status)
                VALUES ('$TABLE_NAME_ESC', true, '$TABLE_DELTA_ESC', $ECC_SQL, 'pending')
                ON CONFLICT (table_name) DO NOTHING
                RETURNING table_name
            " 2>&1 || true)

            if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
                log_success "  $TABLE_NAME : ajoutée"
                ((added+=1))
            else
                log_warning "  $TABLE_NAME : existe déjà ou erreur (ignorée)"
            fi
        done
        echo ""
        log_success "$added table(s) ajoutée(s)"
    else
        # Mode non-interactif : noms passés en arguments
        local added=0
        local skipped=0
        for table in "${tables[@]}"; do
            table="${table^^}"
            local table_esc
            table_esc=$(_pg_escape "$table")
            RESULT=$(_pg_exec -At -c "
                INSERT INTO splus_admin.amue_tables (table_name, enabled, setup_status)
                VALUES ('$table_esc', true, 'pending')
                ON CONFLICT (table_name) DO NOTHING
                RETURNING table_name
            " 2>&1 || true)
            if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
                log_success "  $table : ajoutée"
                ((added+=1))
            else
                log_warning "  $table : existe déjà (ignorée)"
                ((skipped+=1))
            fi
        done
        echo ""
        log_success "$added table(s) ajoutée(s), $skipped ignorée(s)"
    fi

    echo ""
    cmd_list_tables
}

cmd_load_tables() {
    _load_pg_creds
    log_info "Chargement interactif des tables dans splus_admin.amue_tables"
    log_info "(ON CONFLICT DO UPDATE — les entrées existantes sont mises à jour)"
    echo ""

    # Chargement des credentials API depuis la connexion Airflow oauth_api (même pattern que _load_pg_creds)
    local _api_json _api_parsed _api_host _api_client_id _api_client_secret _api_token_url ACCESS_TOKEN ADMIN_ENDPOINT_RESOLVED
    _api_json=$($DOCKER_CMD exec -T airflow-apiserver airflow connections get oauth_api \
        --output json 2>/dev/null)
    _api_parsed=$(echo "$_api_json" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    r = d[0] if isinstance(d, list) else d
    extra = r.get('extra_dejson') or r.get('extra') or {}
    if isinstance(extra, str):
        extra = json.loads(extra)
    if not isinstance(extra, dict):
        extra = {}
    print('\x1f'.join([
        r.get('host') or '',
        r.get('login') or '',
        r.get('password') or '',
        extra.get('token_url') or '',
    ]))
except Exception:
    print('\x1f'.join(['', '', '', '']))
" 2>/dev/null)
    IFS=$'\x1f' read -r _api_host _api_client_id _api_client_secret _api_token_url <<< "$_api_parsed"

    ACCESS_TOKEN=""
    if [[ -n "$_api_client_id" && -n "$_api_client_secret" && -n "$_api_token_url" ]]; then
        _TOKEN_RESP=$(curl -s --max-time 10 -X POST "$_api_token_url" \
            -u "$_api_client_id:$_api_client_secret" \
            -d "grant_type=client_credentials" 2>/dev/null)
        ACCESS_TOKEN=$(echo "$_TOKEN_RESP" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get('access_token', ''))
except Exception:
    pass
" 2>/dev/null || true)
        unset _TOKEN_RESP
    fi

    local _universite _endpoint_admin _api_source _endpoint_entrepot
    _universite=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get universite 2>/dev/null || echo "")
    _endpoint_admin=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get api_endpoint_admin 2>/dev/null || echo "")
    _api_source=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_api_source 2>/dev/null || echo "cdv")
    _endpoint_entrepot=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get api_endpoint_entrepot 2>/dev/null || echo "finances/entrepotdedonnees/v1/preprod/ul")
    ADMIN_ENDPOINT_RESOLVED="${_endpoint_admin/\$\{univ\}/$_universite}"
    _endpoint_entrepot="${_endpoint_entrepot/\$\{univ\}/$_universite}"

    if [[ -n "$ACCESS_TOKEN" ]]; then
        log_info "Token OAuth obtenu — les clés primaires seront récupérées automatiquement depuis l'API (source: $_api_source)"
    else
        log_warning "Token OAuth non disponible — saisie manuelle des clés primaires"
    fi

    local count=0
    while true; do
        echo -n "Ajouter une table ? (o/N) : " >&2
        read -r ADD_TABLE </dev/tty
        [[ ! "$ADD_TABLE" =~ ^[oOyY]$ ]] && break

        echo -n "  Nom de la table : " >&2
        read -r T_NAME </dev/tty
        [[ -z "$T_NAME" ]] && continue
        T_NAME="${T_NAME^^}"

        local T_PK=""
        if [[ -n "$ACCESS_TOKEN" && -n "$_api_host" ]]; then
            if [[ "$_api_source" == "entrepot" && -n "$_endpoint_entrepot" ]]; then
                # API Entrepôt v1.2 : GET /rpc/get_file?nom_table=TABLE.keys  (Accept: text/plain)
                _KEYS_RESP=$(curl -s --max-time 10 \
                    -H "Authorization: Bearer $ACCESS_TOKEN" \
                    -H "Accept: text/plain" \
                    "$_api_host/${_endpoint_entrepot}/rpc/get_file?nom_table=${T_NAME}.keys" 2>/dev/null)
                T_PK=$(echo "$_KEYS_RESP" | python3 -c "
import sys, re
text = sys.stdin.read().strip()
# Valide : liste d'identifiants SQL (MANDT,WERKS) — rejette les messages d'erreur
parts = [p.strip() for p in re.split(r'[,\n\r]+', text) if p.strip()]
if parts and all(re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', p) for p in parts):
    print(','.join(p.upper() for p in parts))
elif text:
    print('WARN:' + text[:120])
" 2>/dev/null || true)
                if [[ "$T_PK" == WARN:* ]]; then
                    log_warning "  API Entrepôt /rpc/get_file : ${T_PK#WARN:}"
                    T_PK=""
                fi
            elif [[ -n "$ADMIN_ENDPOINT_RESOLVED" ]]; then
                # API CDV (historique) : GET /admin?get=TABLE.keys&f=json
                _KEYS_RESP=$(curl -s --max-time 10 \
                    -H "Authorization: Bearer $ACCESS_TOKEN" \
                    "$_api_host/$ADMIN_ENDPOINT_RESOLVED?get=$T_NAME.keys&f=json" 2>/dev/null)
                T_PK=$(echo "$_KEYS_RESP" | python3 -c "
import json, sys
text = sys.stdin.read()
try:
    data = json.loads(text)
    if isinstance(data, list):
        print(','.join(str(k) for k in data if k))
    elif isinstance(data, dict):
        keys = data.get('keys', [])
        print(','.join(str(k) for k in keys if k))
    elif isinstance(data, str):
        print(data.strip())
except (json.JSONDecodeError, ValueError):
    cleaned = text.strip()
    if cleaned and not cleaned.startswith('<'):
        print(cleaned)
" 2>/dev/null || true)
            fi
            unset _KEYS_RESP
        fi
        if [[ -n "$T_PK" ]]; then
            log_info "  Clé primaire récupérée depuis l'API : $T_PK"
            echo -n "  Clé primaire [$T_PK] : " >&2
            read -r _T_PK_INPUT </dev/tty
            [[ -n "$_T_PK_INPUT" ]] && T_PK="$_T_PK_INPUT"
            unset _T_PK_INPUT
        else
            echo -n "  Clé primaire (séparée par virgules, ex: MANDT,BUKRS,BELNR) : " >&2
            read -r T_PK </dev/tty
        fi

        echo -n "  Colonne delta (vide = import complet) : " >&2
        read -r T_DELTA </dev/tty

        local T_NAME_ESC T_PK_ESC T_DELTA_ESC
        T_NAME_ESC=$(_pg_escape "$T_NAME")
        T_PK_ESC=$(_pg_escape "$T_PK")
        T_DELTA_ESC=$(_pg_escape "$T_DELTA")
        RESULT=$(_pg_exec -q -c "
            INSERT INTO splus_admin.amue_tables (table_name, primary_key, delta)
            VALUES ('$T_NAME_ESC', '$T_PK_ESC', '$T_DELTA_ESC')
            ON CONFLICT (table_name) DO UPDATE
              SET primary_key = EXCLUDED.primary_key,
                  delta       = EXCLUDED.delta,
                  updated_at  = NOW();
        " 2>&1 || true)

        echo "$RESULT"

        if [[ "$RESULT" != *"ERROR"* ]]; then
            log_success "  '$T_NAME' enregistrée"
        else
            log_warning "  Erreur pour '$T_NAME'"
        fi

        ((count+=1))
        echo ""
    done

    echo ""
    log_success "$count table(s) enregistrée(s)"
    cmd_list_tables
}

cmd_remove_table() {
    local tables=("$@")
    if [[ ${#tables[@]} -eq 0 ]]; then
        log_error "Usage: ./manage.sh remove-table <table1> [table2] ..."
        echo ""
        cmd_list_tables
        exit 1
    fi

    _load_pg_creds
    local removed=0
    local not_found=0

    for table in "${tables[@]}"; do
        local table_esc
        table_esc=$(_pg_escape "$table")
        RESULT=$(_pg_exec -At -c "
            DELETE FROM splus_admin.amue_tables
            WHERE table_name = '$table_esc'
            RETURNING table_name
        " 2>&1 || true)
        if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
            log_success "  $table : supprimée"
            ((removed+=1))
        else
            log_warning "  $table : non trouvée (ignorée)"
            ((not_found+=1))
        fi
    done

    echo ""
    log_success "$removed table(s) supprimée(s), $not_found non trouvée(s)"
    echo ""
    cmd_list_tables
}

cmd_toggle_table() {
    local tables=("$@")
    if [[ ${#tables[@]} -eq 0 ]]; then
        log_error "Usage: ./manage.sh toggle-table <table1> [table2] ..."
        echo ""
        cmd_list_tables
        exit 1
    fi

    _load_pg_creds
    local toggled=0
    local not_found=()

    for table in "${tables[@]}"; do
        local table_esc
        table_esc=$(_pg_escape "$table")
        RESULT=$(_pg_exec -At -c "
            UPDATE splus_admin.amue_tables
            SET enabled = NOT enabled, updated_at = NOW()
            WHERE table_name = '$table_esc'
            RETURNING table_name, enabled
        " 2>&1 || true)
        if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
            local new_status
            new_status=$(echo "$RESULT" | cut -d'|' -f2)
            if [[ "$new_status" == "t" ]]; then
                log_success "  $table : activée"
            else
                log_info "  $table : désactivée"
            fi
            ((toggled+=1))
        else
            not_found+=("$table")
        fi
    done

    if [[ ${#not_found[@]} -gt 0 ]]; then
        log_warning "Tables non trouvées: ${not_found[*]}"
    fi

    echo ""
    cmd_list_tables
}

cmd_enable_table() {
    local tables=("$@")
    if [[ ${#tables[@]} -eq 0 ]]; then
        log_error "Usage: ./manage.sh enable-table <table1> [table2] ..."
        exit 1
    fi

    _load_pg_creds
    local count=0
    local not_found=()

    for table in "${tables[@]}"; do
        local table_esc
        table_esc=$(_pg_escape "$table")
        RESULT=$(_pg_exec -At -c "
            UPDATE splus_admin.amue_tables
            SET enabled = true, updated_at = NOW()
            WHERE table_name = '$table_esc'
            RETURNING table_name
        " 2>&1 || true)
        if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
            log_success "  $table : activée"
            ((count+=1))
        else
            not_found+=("$table")
        fi
    done

    if [[ ${#not_found[@]} -gt 0 ]]; then
        log_warning "Tables non trouvées: ${not_found[*]}"
    fi
    echo ""
    cmd_list_tables
}

cmd_disable_table() {
    local tables=("$@")
    if [[ ${#tables[@]} -eq 0 ]]; then
        log_error "Usage: ./manage.sh disable-table <table1> [table2] ..."
        exit 1
    fi

    _load_pg_creds
    local count=0
    local not_found=()

    for table in "${tables[@]}"; do
        local table_esc
        table_esc=$(_pg_escape "$table")
        RESULT=$(_pg_exec -At -c "
            UPDATE splus_admin.amue_tables
            SET enabled = false, updated_at = NOW()
            WHERE table_name = '$table_esc'
            RETURNING table_name
        " 2>&1 || true)
        if [[ -n "$RESULT" && "$RESULT" != *"ERROR"* ]]; then
            log_info "  $table : désactivée"
            ((count+=1))
        else
            not_found+=("$table")
        fi
    done

    if [[ ${#not_found[@]} -gt 0 ]]; then
        log_warning "Tables non trouvées: ${not_found[*]}"
    fi
    echo ""
    cmd_list_tables
}

cmd_db_shell() {
    log_info "Connexion au shell PostgreSQL (base métier)..."
    _load_pg_creds
    PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -it postgres-data psql -U "$PG_USER" -d "$PG_DB"
}

cmd_db_backup() {
    _load_pg_creds
    local backup_dir="backups"
    mkdir -p "$backup_dir"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="${backup_dir}/${PG_DB}_${timestamp}.sql"

    log_info "Sauvegarde de la base de données ($PG_DB)..."
    PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data pg_dump -U "$PG_USER" "$PG_DB" > "$backup_file"
    log_success "Sauvegarde créée: $backup_file"
}

cmd_db_restore() {
    local backup_file=${1:-}
    if [[ -z "$backup_file" ]] || [[ ! -f "$backup_file" ]]; then
        log_error "Usage: ./manage.sh db-restore <fichier_backup>"
        log_info "Backups disponibles:"
        ls -lh backups/*.sql 2>/dev/null || log_warning "Aucun backup trouvé"
        exit 1
    fi

    log_warning "ATTENTION: Cette opération va écraser la base de données actuelle!"
    echo -n "Continuer ? (o/N) : " >&2
    read -r _CONFIRM </dev/tty
    if [[ ! "$_CONFIRM" =~ ^[oOyY]$ ]]; then
        log_info "Opération annulée"
        exit 0
    fi

    _load_pg_creds
    log_info "Restauration de la base de données ($PG_DB)..."
    PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data psql -U "$PG_USER" -d "$PG_DB" < "$backup_file"
    log_success "Base de données restaurée"
}

# Résout/valide une release GitHub PUBLIÉE et expose le résultat dans les variables
# globales GITHUB_RELEASE_TAG / GITHUB_RELEASE_NAME (vides + retour != 0 en cas d'échec).
# - Sans argument : récupère la dernière release publiée
# - Avec un tag    : vérifie qu'il correspond bien à une release publiée (pas une simple
#                    branche ou un tag git quelconque) — la mise à jour ne doit cibler
#                    que des releases GitHub.
_resolve_github_release() {
    local requested="${1:-}"
    GITHUB_RELEASE_TAG=""
    GITHUB_RELEASE_NAME=""

    # Rapatrie les tags depuis le remote (utilise les credentials git existants —
    # SSH, HTTPS avec credentials helper, etc. — aucun token supplémentaire requis)
    log_info "Récupération des tags depuis le remote..."
    if ! git fetch --tags --quiet 2>/dev/null; then
        log_warning "'git fetch --tags' a échoué — utilisation des tags locaux uniquement"
    fi

    if [[ -n "$requested" ]]; then
        # Vérifie que le tag demandé existe
        if ! git rev-parse "$requested" > /dev/null 2>&1; then
            log_error "Le tag '$requested' est introuvable (localement et après fetch)"
            log_info "Tags disponibles : $(git tag --sort=-version:refname | head -10 | tr '\n' ' ')"
            return 1
        fi
        GITHUB_RELEASE_TAG="$requested"
    else
        # Prend le tag le plus récent (tri sémantique)
        GITHUB_RELEASE_TAG=$(git tag --sort=-version:refname 2>/dev/null | head -1)
        if [[ -z "$GITHUB_RELEASE_TAG" ]]; then
            log_error "Aucun tag trouvé dans le dépôt — créez une release/tag avant de lancer update"
            return 1
        fi
    fi

    # Pas de nom de release via git pur — GITHUB_RELEASE_NAME reste vide
    return 0
}

# Demande interactivement la source d'API AMUE, met à jour la variable Airflow et le template.
# Ne fait rien si la variable est déjà définie dans Airflow (mode update silencieux).
# Passe --force pour forcer la reconfiguration même si déjà définie.
_configure_api_source() {
    local force="${1:-}"

    # Vérifie si la variable est déjà définie dans Airflow
    local current_source
    current_source=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_api_source 2>/dev/null || true)

    if [[ -n "$current_source" && "$force" != "--force" ]]; then
        log_info "Source API déjà configurée : $current_source (utilisez --force pour modifier)"
        return 0
    fi

    # Si déjà configurée et --force, affiche la valeur courante
    if [[ -n "$current_source" ]]; then
        log_info "Source API actuelle : $current_source"
    fi

    # Prompt interactif
    echo ""
    echo -e "${CYAN}Quelle source d'API AMUE utiliser ?${NC}" >&2
    echo "  1) CDV      — API v1 classique (finances/cdv/v1/...)" >&2
    echo "  2) Entrepôt — API v1.2 PostgREST (finances/entrepotdedonnees/v1/...)" >&2
    local default_choice="1"
    [[ "$current_source" == "entrepot" ]] && default_choice="2"
    echo -n "Votre choix [$default_choice] : " >&2
    local choice
    read -r choice </dev/tty
    choice="${choice:-$default_choice}"

    local new_source
    case "$choice" in
        1|cdv)      new_source="cdv" ;;
        2|entrepot) new_source="entrepot" ;;
        *)
            log_error "Choix invalide — valeurs acceptées : 1 (cdv) ou 2 (entrepot)"
            return 1
            ;;
    esac

    # Mise à jour dans Airflow
    $DOCKER_CMD exec -T airflow-apiserver airflow variables set amue_api_source "$new_source" > /dev/null 2>&1
    log_success "Source API définie : $new_source"

    # Mise à jour du template config/airflow_variables.json (pour les prochains syncs)
    local template="config/airflow_variables.json"
    if [[ -f "$template" ]]; then
        python3 -c "
import json, sys
with open('$template', encoding='utf-8') as f:
    data = json.load(f)
data['amue_api_source'] = '$new_source'
with open('$template', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
    f.write('\n')
" && log_info "Template $template mis à jour"
    fi
}

# Synchronise les variables Airflow depuis config/airflow_variables.json de façon
# "diff-based" : n'écrase JAMAIS une valeur existante, n'ajoute/ne met à jour que
# les clés nouvelles ou dont la valeur a changé par rapport au référentiel versionné.
# Les clés personnalisées par l'opérateur (absentes du template) sont préservées telles quelles.
_sync_variables() {
    local template="config/airflow_variables.json"
    if [[ ! -f "$template" ]]; then
        log_warning "$template introuvable — synchronisation des variables ignorée"
        return 0
    fi

    local current_json
    current_json=$($DOCKER_CMD exec -T airflow-apiserver airflow variables export - 2>/dev/null)

    local diff_output
    diff_output=$(echo "$current_json" | python3 -c "
import json, sys

with open('$template', encoding='utf-8') as f:
    template = json.load(f)

try:
    current = json.load(sys.stdin)
except Exception:
    current = {}
if isinstance(current, list):
    current = {item['key']: item.get('val', '') for item in current}

for key, new_val in template.items():
    new_val = new_val if isinstance(new_val, str) else json.dumps(new_val)
    if key not in current:
        print(f'NEW\x1f{key}\x1f{new_val}')
    elif current[key] != new_val:
        print(f'CHANGED\x1f{key}\x1f{new_val}')
" 2>/dev/null)

    if [[ -z "$diff_output" ]]; then
        log_info "Variables Airflow déjà à jour — rien à synchroniser"
        return 0
    fi

    local action key value count=0
    while IFS=$'\x1f' read -r action key value; do
        [[ -z "$key" ]] && continue
        if [[ "$action" == "NEW" ]]; then
            log_info "  + nouvelle variable : $key"
        else
            log_info "  ~ variable mise à jour (template modifié) : $key"
        fi
        $DOCKER_CMD exec -T airflow-apiserver airflow variables set "$key" "$value" > /dev/null 2>&1
        ((count+=1))
    done <<< "$diff_output"

    log_success "$count variable(s) synchronisée(s) depuis $template"
}

# Écrit le fichier marqueur de progression de cmd_update (étape atteinte + contexte de reprise/rollback)
_update_write_marker() {
    local marker="$1" step="$2" tag="$3" prev_ref="$4" db_backup="$5" config_backup="$6"
    mkdir -p backups
    cat > "$marker" << EOF
UPDATE_LAST_STEP=$step
UPDATE_TARGET_TAG=$tag
UPDATE_PREVIOUS_REF=$prev_ref
UPDATE_DB_BACKUP=$db_backup
UPDATE_CONFIG_BACKUP=$config_backup
EOF
}

# Affiche les instructions de reprise/rollback quand une étape de cmd_update échoue
_update_fail() {
    local marker="$1" step_num="$2" total_steps="$3" step_name="$4" \
          tag="$5" prev_ref="$6" db_backup="$7" config_backup="$8"
    _update_write_marker "$marker" "$((step_num - 1))" "$tag" "$prev_ref" "$db_backup" "$config_backup"
    echo "" >&2
    log_error "Mise à jour interrompue à l'étape $step_num/$total_steps ($step_name)."
    log_info "Pour corriger le problème puis reprendre : ./manage.sh update $tag --resume"
    log_info "Pour annuler complètement la mise à jour :"
    log_info "  git checkout $prev_ref"
    [[ -n "$db_backup" ]] && log_info "  ./manage.sh db-restore $db_backup"
    [[ -n "$config_backup" ]] && log_info "  ./manage.sh config-restore $config_backup"
    log_info "  ./manage.sh build && ./manage.sh start"
}

# Met à jour le PROJET DANS SON ENSEMBLE (code, dépendances/image Docker, schéma SQL
# applicatif, métadonnées Airflow, variables Airflow) à partir d'un TAG GIT.
# Utilise git fetch/checkout directement — aucun GITHUB_TOKEN requis.
#   ./manage.sh update              -> prend le tag le plus récent (tri sémantique)
#   ./manage.sh update v1.2.0       -> vérifie que v1.2.0 existe comme tag git
#   ./manage.sh update v1.2.0 --resume -> reprend une mise à jour interrompue à l'étape v1.2.0
cmd_update() {
    local target="" resume=false
    local arg
    for arg in "$@"; do
        case "$arg" in
            --resume) resume=true ;;
            *) [[ -z "$target" ]] && target="$arg" ;;
        esac
    done

    local marker="backups/.update_in_progress"
    local total_steps=9
    local current_ref
    current_ref=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
    [[ "$current_ref" == "HEAD" ]] && current_ref=$(git rev-parse --short HEAD 2>/dev/null)

    local last_step=0 db_backup_file="" config_backup_file="" release_tag=""

    if [[ "$resume" == true && -f "$marker" ]]; then
        # shellcheck disable=SC1090
        source "$marker"
        last_step="${UPDATE_LAST_STEP:-0}"
        db_backup_file="${UPDATE_DB_BACKUP:-}"
        config_backup_file="${UPDATE_CONFIG_BACKUP:-}"
        release_tag="${UPDATE_TARGET_TAG:-}"
        current_ref="${UPDATE_PREVIOUS_REF:-$current_ref}"
        log_info "Reprise d'une mise à jour interrompue après l'étape $last_step (marqueur : $marker)"
        [[ -n "$target" && -n "$release_tag" && "$target" != "$release_tag" ]] \
            && log_warning "Cible '$target' ignorée — reprise de la mise à jour vers '$release_tag' déjà en cours"
        target="$release_tag"
    fi

    # ---- Étape 1/9 : résolution du tag cible + pré-checks ----
    if [[ "$last_step" -lt 1 ]]; then
        log_info "[ÉTAPE 1/$total_steps] Résolution du tag cible et vérifications préalables..."

        if ! _resolve_github_release "$target"; then
            return 1
        fi
        release_tag="$GITHUB_RELEASE_TAG"
        log_success "Tag cible : $release_tag"

        if [[ -n "$(git status --porcelain)" ]]; then
            log_error "Arbre de travail git non propre — committez ou annulez vos changements avant la mise à jour"
            git status --short
            return 1
        fi

        log_info "Vérification de l'état de santé actuel (référence avant mise à jour)..."
        cmd_health || log_warning "L'état de santé actuel n'est pas entièrement vert — la mise à jour peut tout de même continuer"

        _update_write_marker "$marker" 1 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 1/$total_steps] déjà effectuée (reprise) — tag cible : $release_tag"
    fi

    # ---- Étape 2/9 : confirmation ----
    if [[ "$last_step" -lt 2 ]]; then
        log_info "[ÉTAPE 2/$total_steps] Confirmation..."

        local current_version target_version
        current_version=$(cat VERSION 2>/dev/null || echo "inconnue")
        target_version=$(git show "${release_tag}:VERSION" 2>/dev/null || echo "inconnue")

        echo "" >&2
        log_info "Version actuelle  : $current_version (réf. git : $current_ref)"
        log_info "Version cible     : $target_version (release : $release_tag)"
        [[ -n "$GITHUB_RELEASE_NAME" ]] && log_info "Nom de la release : $GITHUB_RELEASE_NAME"

        local migrations_dir="scripts/sql/migrations"
        if [[ -d "$migrations_dir" ]]; then
            local migration_count
            migration_count=$(find "$migrations_dir" -maxdepth 1 -name '[0-9][0-9][0-9][0-9]_*.sql' 2>/dev/null | wc -l | tr -d ' ')
            [[ "$migration_count" -gt 0 ]] && log_info "Migrations présentes dans le dépôt : $migration_count (statut exact vérifié à l'étape 8)"
        fi

        echo "" >&2
        log_warning "Cette opération va arrêter les services, mettre à jour le code, reconstruire l'image,"
        log_warning "appliquer les migrations SQL et redémarrer. Des sauvegardes seront créées avant toute action destructive."
        echo -n "Continuer la mise à jour vers $release_tag ? (o/N) : " >&2
        read -r _CONFIRM </dev/tty
        if [[ ! "$_CONFIRM" =~ ^[oOyY]$ ]]; then
            log_info "Mise à jour annulée"
            return 0
        fi

        _update_write_marker "$marker" 2 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 2/$total_steps] déjà effectuée (reprise)"
    fi

    # ---- Étape 3/9 : sauvegardes ----
    if [[ "$last_step" -lt 3 ]]; then
        log_info "[ÉTAPE 3/$total_steps] Sauvegarde de la base de données et de la configuration..."
        mkdir -p backups backups/config

        if ! cmd_db_backup; then
            _update_fail "$marker" 3 "$total_steps" "sauvegarde de la base de données" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        db_backup_file=$(ls -t backups/*.sql 2>/dev/null | head -1)

        if ! cmd_config_backup; then
            _update_fail "$marker" 3 "$total_steps" "sauvegarde de la configuration" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        config_backup_file=$(ls -t backups/config/*.tar.gz 2>/dev/null | head -1)

        log_success "Sauvegardes prêtes : $db_backup_file / $config_backup_file"
        _update_write_marker "$marker" 3 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 3/$total_steps] déjà effectuée (reprise) — sauvegardes : $db_backup_file / $config_backup_file"
    fi

    # ---- Étape 4/9 : arrêt des services ----
    if [[ "$last_step" -lt 4 ]]; then
        log_info "[ÉTAPE 4/$total_steps] Arrêt des services..."
        if ! cmd_stop; then
            _update_fail "$marker" 4 "$total_steps" "arrêt des services" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        _update_write_marker "$marker" 4 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 4/$total_steps] déjà effectuée (reprise)"
    fi

    # ---- Étape 5/9 : mise à jour du code ----
    if [[ "$last_step" -lt 5 ]]; then
        log_info "[ÉTAPE 5/$total_steps] Mise à jour du code vers $release_tag..."
        if ! git checkout "$release_tag"; then
            _update_fail "$marker" 5 "$total_steps" "checkout de la release '$release_tag'" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        log_success "Code à jour : $release_tag"
        _update_write_marker "$marker" 5 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 5/$total_steps] déjà effectuée (reprise) — code déjà sur $release_tag"
    fi

    # ---- Étape 6/9 : reconstruction de l'image Docker ----
    if [[ "$last_step" -lt 6 ]]; then
        log_info "[ÉTAPE 6/$total_steps] Reconstruction de l'image Docker (code + dépendances Python)..."
        if ! cmd_build; then
            _update_fail "$marker" 6 "$total_steps" "reconstruction de l'image Docker" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        _update_write_marker "$marker" 6 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 6/$total_steps] déjà effectuée (reprise)"
    fi

    # ---- Étape 7/9 : redémarrage ----
    if [[ "$last_step" -lt 7 ]]; then
        log_info "[ÉTAPE 7/$total_steps] Redémarrage des services..."
        log_info "Le service airflow-init exécute automatiquement 'airflow db migrate' au démarrage (_AIRFLOW_DB_MIGRATE=true)"
        if ! cmd_start; then
            _update_fail "$marker" 7 "$total_steps" "redémarrage des services" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        _update_write_marker "$marker" 7 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 7/$total_steps] déjà effectuée (reprise)"
    fi

    # ---- Étape 8/9 : migrations SQL applicatives + synchro variables ----
    if [[ "$last_step" -lt 8 ]]; then
        log_info "[ÉTAPE 8/$total_steps] Application des migrations SQL et synchronisation des variables Airflow..."
        if ! cmd_db_migrate; then
            _update_fail "$marker" 8 "$total_steps" "migrations SQL applicatives" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        # Configure la source API si pas encore définie (première mise à jour après ajout de la variable)
        _configure_api_source
        if ! _sync_variables; then
            _update_fail "$marker" 8 "$total_steps" "synchronisation des variables Airflow" "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
            return 1
        fi
        _update_write_marker "$marker" 8 "$release_tag" "$current_ref" "$db_backup_file" "$config_backup_file"
    else
        log_info "[ÉTAPE 8/$total_steps] déjà effectuée (reprise)"
    fi

    # ---- Étape 9/9 : vérification finale ----
    log_info "[ÉTAPE 9/$total_steps] Vérification finale..."
    cmd_health
    cmd_verify || log_warning "cmd_verify a signalé des avertissements — vérifiez la sortie ci-dessus"

    rm -f "$marker"

    echo "" >&2
    log_success "Mise à jour terminée : $current_ref -> $release_tag"
    log_info "Sauvegardes conservées : $db_backup_file / $config_backup_file"
    log_info "Pour revenir en arrière si besoin, voir docs/UPGRADE.md (section Rollback)"
}

cmd_test() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh test <dag_id>"
        exit 1
    fi

    log_info "Test du DAG $dag_id..."
    airflow_exec dags test "$dag_id"
}

cmd_test_email() {
    log_info "Test de la configuration email..."
    chmod +x scripts/dev/test_email.sh
    ./scripts/dev/test_email.sh
}

cmd_shell() {
    log_info "Shell interactif dans le container airflow-apiserver..."
    $DOCKER_CMD exec -it airflow-apiserver bash
}

cmd_python() {
    log_info "Console Python interactive..."
    $DOCKER_CMD exec -it airflow-apiserver python
}

cmd_clean() {
    log_info "Nettoyage des fichiers temporaires..."

    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    find . -type f -name "*.pyo" -delete 2>/dev/null || true
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

    log_success "Nettoyage terminé"
}

cmd_version() {
    log_info "Version applicative:"
    echo ""

    local app_version
    app_version=$(cat VERSION 2>/dev/null || git describe --tags 2>/dev/null || echo "inconnue")
    echo "DemoDAGS: $app_version"
    echo "Référence git: $(git rev-parse --abbrev-ref HEAD 2>/dev/null) ($(git rev-parse --short HEAD 2>/dev/null))"

    if $DOCKER_CMD ps 2>/dev/null | grep -q "postgres-data"; then
        _load_pg_creds
        local migrations
        migrations=$(PGPASSWORD="$PG_PASSWORD" $DOCKER_CMD exec -T postgres-data \
            psql -U "$PG_USER" -d "$PG_DB" -t -A -F ' | ' \
            -c "SELECT version, description, applied_at FROM splus_admin.schema_migrations ORDER BY version DESC LIMIT 5;" 2>/dev/null)
        if [[ -n "$migrations" ]]; then
            echo ""
            echo "Dernières migrations appliquées (splus_admin.schema_migrations) :"
            echo "$migrations" | while IFS= read -r line; do echo "  $line"; done
        fi
    fi

    echo ""
    log_info "Versions runtime:"
    echo ""

    echo -n "Docker: "
    docker --version

    echo -n "Docker Compose: "
    $DOCKER_CMD version --short 2>/dev/null || echo "N/A"

    if $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
        $DOCKER_CMD exec -T airflow-apiserver bash -c "
            echo -n 'Airflow: '; airflow version 2>/dev/null || echo 'N/A'
            echo -n 'Python: '; python --version 2>/dev/null || echo 'N/A'
        "
    else
        log_warning "Services Airflow non démarrés"
    fi
}

###############################################################################
# Monitoring & Debug
###############################################################################

cmd_health() {
    log_info "Vérification de la santé des services..."
    echo ""

    # Vérification des containers
    log_info "=== État des containers ==="
    $DOCKER_CMD ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

    echo ""
    log_info "=== Santé Airflow ==="

    # API Server
    if curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
        log_success "API Server: OK"
    else
        log_error "API Server: KO"
    fi

    # Scheduler — hostname évalué dans le container (pas sur le host)
    if $DOCKER_CMD exec -T airflow-scheduler bash -c \
        'airflow jobs check --job-type SchedulerJob --hostname "$(hostname)"' 2>/dev/null; then
        log_success "Scheduler: OK"
    else
        log_warning "Scheduler: vérification manuelle requise"
    fi

    # Base de données
    if $DOCKER_CMD exec -T postgres pg_isready -q > /dev/null 2>&1; then
        log_success "PostgreSQL Airflow: OK"
    else
        log_error "PostgreSQL Airflow: KO"
    fi

    if $DOCKER_CMD exec -T postgres-data pg_isready -q > /dev/null 2>&1; then
        log_success "PostgreSQL Data: OK"
    else
        log_error "PostgreSQL Data: KO"
    fi

    echo ""
    log_info "=== Métriques ==="
    local _dags_json total active
    _dags_json=$($DOCKER_CMD exec -T airflow-apiserver airflow dags list --output json 2>/dev/null || echo "[]")
    read -r total active < <(echo "$_dags_json" | python3 -c "
import json, sys
lst = json.load(sys.stdin)
lst = lst if isinstance(lst, list) else []
print(len(lst), sum(1 for x in lst if not x.get('is_paused', True)))
" 2>/dev/null || echo "0 0")
    echo "DAGs totaux : $total"
    echo "DAGs actifs : $active"
}

cmd_failed() {
    local limit=${1:-20}
    log_info "Tâches en échec récentes (limite: $limit)..."
    echo ""

    $DOCKER_CMD exec -T airflow-apiserver airflow tasks failed-deps --limit "$limit" 2>/dev/null || \
    $DOCKER_CMD exec -T airflow-apiserver bash -c "
        airflow dags list-runs --state failed -o table 2>/dev/null | head -$((limit + 2))
    " || log_warning "Impossible de récupérer les tâches en échec"
}

cmd_task_logs() {
    local dag_id=${1:-}
    local task_id=${2:-}
    local run_id=${3:-}

    if [[ -z "$dag_id" ]] || [[ -z "$task_id" ]]; then
        log_error "Usage: ./manage.sh task-logs <dag_id> <task_id> [run_id]"
        echo ""
        log_info "Exemple: ./manage.sh task-logs my_dag my_task"
        exit 1
    fi

    log_info "Logs de la tâche $task_id du DAG $dag_id..."

    if [[ -n "$run_id" ]]; then
        airflow_exec tasks logs "$dag_id" "$task_id" "$run_id"
    else
        # Récupère le dernier run_id — dag_id et task_id passés en args positionnels (évite l'injection)
        log_info "Recherche du dernier run..."
        $DOCKER_CMD exec -T airflow-apiserver bash -c '
            LAST_RUN=$(airflow dags list-runs -d "$1" -o plain 2>/dev/null | head -1 | awk "{print \$3}")
            if [[ -n "$LAST_RUN" ]]; then
                airflow tasks logs "$1" "$2" "$LAST_RUN"
            else
                echo "Aucun run trouvé pour ce DAG"
            fi
        ' _ "$dag_id" "$task_id"
    fi
}

###############################################################################
# Gestion des DAGs
###############################################################################

cmd_pause_all() {
    log_info "Mise en pause de tous les DAGs..."

    $DOCKER_CMD exec -T airflow-apiserver bash -c "
        airflow dags list -o plain 2>/dev/null | tail -n +2 | while read dag_id _; do
            airflow dags pause \"\$dag_id\" 2>/dev/null && echo \"  - \$dag_id: pausé\"
        done
    "

    log_success "Tous les DAGs sont en pause"
}

cmd_unpause_all() {
    log_info "Réactivation de tous les DAGs..."

    $DOCKER_CMD exec -T airflow-apiserver bash -c "
        airflow dags list -o plain 2>/dev/null | tail -n +2 | while read dag_id _; do
            airflow dags unpause \"\$dag_id\" 2>/dev/null && echo \"  - \$dag_id: actif\"
        done
    "

    log_success "Tous les DAGs sont actifs"
}

cmd_backfill() {
    local dag_id=${1:-}
    local start_date=${2:-}
    local end_date=${3:-}

    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh backfill <dag_id> [start_date] [end_date]"
        echo ""
        log_info "Formats de date: YYYY-MM-DD"
        log_info "Exemple: ./manage.sh backfill my_dag 2024-01-01 2024-01-31"
        exit 1
    fi

    if [[ -z "$start_date" ]]; then
        start_date=$(date -d "7 days ago" +%Y-%m-%d 2>/dev/null || date -v-7d +%Y-%m-%d)
    fi

    if [[ -z "$end_date" ]]; then
        end_date=$(date +%Y-%m-%d)
    fi

    log_info "Backfill du DAG $dag_id du $start_date au $end_date..."
    log_warning "Cette opération peut prendre du temps..."

    airflow_exec dags backfill \
        --start-date "$start_date" \
        --end-date "$end_date" \
        "$dag_id"

    log_success "Backfill terminé"
}

###############################################################################
# Maintenance
###############################################################################

cmd_cleanup_logs() {
    local days=${1:-30}
    log_info "Nettoyage des logs de plus de $days jours..."

    # Logs locaux
    local count_local
    count_local=$(find logs -type f -mtime +$days 2>/dev/null | wc -l)
    find logs -type f -mtime +$days -delete 2>/dev/null || true
    find logs -type d -empty -delete 2>/dev/null || true
    log_success "Logs locaux supprimés: $count_local fichiers"

    # Logs dans le container
    $DOCKER_CMD exec -T airflow-apiserver bash -c "
        count=\$(find /opt/airflow/logs -type f -mtime +$days 2>/dev/null | wc -l)
        find /opt/airflow/logs -type f -mtime +$days -delete 2>/dev/null || true
        find /opt/airflow/logs -type d -empty -delete 2>/dev/null || true
        echo \"Logs container supprimés: \$count fichiers\"
    "

    log_success "Nettoyage des logs terminé"
}

cmd_cleanup_db() {
    local days=${1:-30}
    log_info "Purge des données Airflow de plus de $days jours..."
    log_warning "Cette opération supprime les anciennes exécutions de la base Airflow"

    echo -n "Continuer ? (o/N) : " >&2
    read -r CONFIRM </dev/tty
    [[ ! "$CONFIRM" =~ ^[oOyY]$ ]] && { log_info "Annulé"; exit 0; }

    local cutoff_date
    # Validate $days is a positive integer before use
    if [[ ! "$days" =~ ^[0-9]+$ ]]; then
        log_error "Valeur invalide pour le nombre de jours : '$days'"
        exit 1
    fi
    cutoff_date=$(date -d "$days days ago" +%Y-%m-%d 2>/dev/null \
        || date -v-${days}d +%Y-%m-%d 2>/dev/null \
        || { log_error "Commande date incompatible — impossible de calculer la date"; exit 1; })

    airflow_exec db clean \
        --clean-before-timestamp "$cutoff_date" \
        --yes

    log_success "Purge terminée"
}

cmd_reset() {
    log_warning "ATTENTION: Cette opération va supprimer tous les volumes et recréer l'environnement!"
    log_warning "Toutes les données seront perdues (base de données, logs, etc.)"
    echo ""

    echo -n "Êtes-vous sûr ? Tapez 'RESET' pour confirmer : " >&2
    read -r CONFIRM </dev/tty

    if [[ "$CONFIRM" != "RESET" ]]; then
        log_info "Opération annulée"
        exit 0
    fi

    log_info "Reset complet en cours..."

    $DOCKER_CMD down -v

    _build_image
    log_info "Redémarrage des services..."
    $DOCKER_CMD up -d

    log_info "Attente que l'API Airflow soit disponible..."
    local _retries=36
    while [[ $_retries -gt 0 ]]; do
        if curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
            log_success "API Airflow disponible"
            break
        fi
        _retries=$(( _retries - 1 ))
        [[ $_retries -eq 0 ]] && log_warning "API Airflow non disponible après 3 min — vérifiez les logs"
        sleep 5
    done

    log_success "Reset terminé"
    log_info "Relancez './manage.sh config' pour reconfigurer les variables et connexions Airflow"
    cmd_status
}

###############################################################################
# Développement
###############################################################################

cmd_validate() {
    log_info "Validation des DAGs..."
    echo ""

    # Comptage via JSON (evite de compter l'en-tete du tableau texte)
    local _errors_json error_count
    _errors_json=$($DOCKER_CMD exec -T airflow-apiserver \
        airflow dags list-import-errors --output json 2>/dev/null || echo "[]")
    error_count=$(echo "$_errors_json" | \
        python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else 0)" \
        2>/dev/null || echo "0")

    if [[ "$error_count" -eq 0 ]]; then
        log_success "Tous les DAGs sont valides"
    else
        # Affichage texte uniquement en cas d'erreurs (evite le double appel dans le cas nominal)
        airflow_exec dags list-import-errors
        log_error "$error_count erreur(s) d'import détectée(s)"
    fi

    echo ""
    log_info "Test de parsing des DAGs..."
    $DOCKER_CMD exec -T airflow-apiserver bash -c "
        for dag_file in /opt/airflow/dags/*.py; do
            if [[ -f \"\$dag_file\" ]]; then
                python3 -m py_compile \"\$dag_file\" 2>&1 && echo \"  ✓ \$(basename \$dag_file)\" || echo \"  ✗ \$(basename \$dag_file)\"
            fi
        done
    "
}

cmd_lint() {
    log_info "Analyse du code des DAGs..."
    echo ""

    # Vérifie si ruff ou flake8 est disponible
    if $DOCKER_CMD exec -T airflow-apiserver command -v ruff &> /dev/null; then
        log_info "Utilisation de ruff..."
        $DOCKER_CMD exec -T airflow-apiserver ruff check /opt/airflow/dags/ || true
    elif $DOCKER_CMD exec -T airflow-apiserver command -v flake8 &> /dev/null; then
        log_info "Utilisation de flake8..."
        $DOCKER_CMD exec -T airflow-apiserver flake8 /opt/airflow/dags/ --max-line-length=120 || true
    else
        log_warning "Aucun linter disponible (ruff, flake8)"
        log_warning "Installation de ruff dans le container (sera perdu au prochain restart)..."
        $DOCKER_CMD exec -T airflow-apiserver pip install ruff --quiet
        $DOCKER_CMD exec -T airflow-apiserver ruff check /opt/airflow/dags/ || true
    fi
}

cmd_tests() {
    local test_path=${1:-}
    local verbose=${2:-}

    log_info "Lancement des tests..."
    echo ""

    # Utilise le venv du projet s'il existe (Linux ou Windows)
    local VENV_PYTEST="$SCRIPT_DIR/.venv/bin/pytest"
    local VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

    # Format Windows
    if [[ -f "$SCRIPT_DIR/.venv/Scripts/pytest.exe" ]]; then
        VENV_PYTEST="$SCRIPT_DIR/.venv/Scripts/pytest.exe"
        VENV_PYTHON="$SCRIPT_DIR/.venv/Scripts/python.exe"
    fi

    if [[ -f "$VENV_PYTEST" ]]; then
        # Tests avec le venv local
        log_info "Exécution avec le venv local"
        local _pytest_target="tests/"
        [[ -n "$test_path" && "$test_path" != "-v" ]] && _pytest_target="tests/$test_path"
        "$VENV_PYTEST" "$_pytest_target" -v --tb=short
    elif command -v pytest &> /dev/null; then
        # Tests avec pytest global
        log_info "Exécution avec pytest global"
        pytest tests/ -v --tb=short
    else
        # Tests dans le container
        log_info "Exécution dans le container"

        # Vérifie si pytest est installé dans le container
        if ! $DOCKER_CMD exec -T airflow-apiserver command -v pytest &> /dev/null; then
            log_info "Installation de pytest..."
            $DOCKER_CMD exec -T airflow-apiserver python -m pip install pytest pytest-mock --quiet
        fi

        # Copie les tests dans le container si nécessaire
        if [[ ! -d "tests" ]]; then
            log_error "Dossier tests/ introuvable"
            exit 1
        fi
        $DOCKER_CMD cp tests airflow-apiserver:/opt/airflow/tests 2>/dev/null || true

        if [[ -n "$test_path" ]] && [[ "$test_path" != "-v" ]]; then
            $DOCKER_CMD exec -T airflow-apiserver pytest /opt/airflow/tests/"$test_path" -v --tb=short
        else
            $DOCKER_CMD exec -T airflow-apiserver pytest /opt/airflow/tests/ -v --tb=short
        fi
    fi
}

cmd_tests_coverage() {
    log_info "Lancement des tests avec couverture..."
    echo ""

    # Utilise le venv du projet s'il existe (Linux ou Windows)
    local VENV_PYTEST="$SCRIPT_DIR/.venv/bin/pytest"
    local VENV_PIP="$SCRIPT_DIR/.venv/bin/pip"
    local VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

    # Format Windows
    if [[ -f "$SCRIPT_DIR/.venv/Scripts/pytest.exe" ]]; then
        VENV_PYTEST="$SCRIPT_DIR/.venv/Scripts/pytest.exe"
        VENV_PIP="$SCRIPT_DIR/.venv/Scripts/pip.exe"
        VENV_PYTHON="$SCRIPT_DIR/.venv/Scripts/python.exe"
    fi

    if [[ -f "$VENV_PYTEST" ]]; then
        # Vérifie si pytest-cov est installé
        if ! "$VENV_PYTHON" -c "import pytest_cov" 2>/dev/null; then
            log_info "Installation de pytest-cov..."
            "$VENV_PIP" install pytest-cov --quiet
        fi

        "$VENV_PYTEST" tests/ -v --cov=dags --cov=plugins --cov-report=term-missing --cov-report=html

        log_success "Rapport de couverture généré dans htmlcov/"
    elif command -v pytest &> /dev/null; then
        pytest tests/ -v --cov=dags --cov=plugins --cov-report=term-missing --cov-report=html
        log_success "Rapport de couverture généré dans htmlcov/"
    else
        log_warning "pytest non disponible"
        log_info "Créez un venv avec: python3 -m venv .venv && .venv/bin/pip install pytest pytest-cov"
    fi
}

###############################################################################
# Point d'entrée
###############################################################################

main() {
    show_banner

    local command=${1:-help}
    shift || true

    case "$command" in
        # Gestion des services
        build)          cmd_build "$@" ;;
        start)          cmd_start "$@" ;;
        stop)           cmd_stop "$@" ;;
        restart)        cmd_restart "$@" ;;
        refresh-plugins)        cmd_refresh_plugins "$@" ;;
        status)         cmd_status "$@" ;;
        logs)           cmd_logs "$@" ;;
        health)         cmd_health "$@" ;;

        # Configuration
        setup)          cmd_setup "$@" ;;
        setup-bluegreen) cmd_setup_bluegreen "$@" ;;
        config)         cmd_config "$@" ;;
        fix)            cmd_fix "$@" ;;
        auto-fix)       cmd_auto_fix "$@" ;;
        verify)         cmd_verify "$@" ;;
        export)         cmd_export "$@" ;;
        diagnose)       cmd_diagnose "$@" ;;
        test-config)    cmd_test_config "$@" ;;

        # Airflow - DAGs
        dags)           cmd_dags "$@" ;;
        trigger)        cmd_trigger "$@" ;;
        pause)          cmd_pause "$@" ;;
        unpause)        cmd_unpause "$@" ;;
        pause-all)      cmd_pause_all "$@" ;;
        unpause-all)    cmd_unpause_all "$@" ;;
        backfill)       cmd_backfill "$@" ;;

        # Airflow - Monitoring
        failed)         cmd_failed "$@" ;;
        task-logs)      cmd_task_logs "$@" ;;
        validate)       cmd_validate "$@" ;;
        lint)           cmd_lint "$@" ;;

        # Airflow - Ressources
        variables)      cmd_variables "$@" ;;
        connections)    cmd_connections "$@" ;;
        users)          cmd_users "$@" ;;
        add-user)       cmd_add_user "$@" ;;
        delete-user)    cmd_delete_user "$@" ;;
        add-table)      cmd_add_table "$@" ;;
        load-tables)    cmd_load_tables ;;
        list-tables)    cmd_list_tables "$@" ;;
        remove-table)   cmd_remove_table "$@" ;;
        toggle-table)   cmd_toggle_table "$@" ;;
        enable-table)   cmd_enable_table "$@" ;;
        disable-table)  cmd_disable_table "$@" ;;

        # Gestion des variables
        var-get)        cmd_var_get "$@" ;;
        var-set)        cmd_var_set "$@" ;;
        var-delete)     cmd_var_delete "$@" ;;
        var-export)     cmd_var_export "$@" ;;
        var-import)     cmd_var_import "$@" ;;

        # Gestion des connexions
        conn-test)      cmd_conn_test "$@" ;;
        conn-export)    cmd_conn_export "$@" ;;
        conn-update)    cmd_conn_update "$@" ;;

        # Configuration globale
        config-validate)  cmd_config_validate "$@" ;;
        config-backup)    cmd_config_backup "$@" ;;
        config-restore)   cmd_config_restore "$@" ;;
        api-source)       _configure_api_source "${1:---}" ;;

        # Base de données
        db-shell)       cmd_db_shell "$@" ;;
        db-backup)      cmd_db_backup "$@" ;;
        db-restore)     cmd_db_restore "$@" ;;
        db-migrate)     cmd_db_migrate "$@" ;;
        update)         cmd_update "$@" ;;

        # Maintenance
        cleanup-logs)   cmd_cleanup_logs "$@" ;;
        cleanup-db)     cmd_cleanup_db "$@" ;;
        reset)          cmd_reset "$@" ;;
        clean)          cmd_clean "$@" ;;

        # Développement
        test)           cmd_test "$@" ;;
        test-email)     cmd_test_email "$@" ;;
        tests)          cmd_tests "$@" ;;
        tests-cov)      cmd_tests_coverage "$@" ;;
        shell)          cmd_shell "$@" ;;
        python)         cmd_python "$@" ;;

        # Autres
        version)        cmd_version "$@" ;;
        help|--help|-h) show_help ;;

        *)
            log_error "Commande inconnue: $command"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"