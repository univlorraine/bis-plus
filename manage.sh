#!/bin/bash

###############################################################################
# Script de gestion Airflow AMUE
# Centralise toutes les opérations courantes
###############################################################################

set -e

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Détecte docker-compose ou docker compose
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

###############################################################################
# Fonctions
###############################################################################

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
    start               Démarre tous les services
    stop                Arrête tous les services
    restart             Redémarre tous les services
    status              Affiche l'état des services
    logs [service]      Affiche les logs (optionnel: nom du service)

  CONFIGURATION
    setup               Installation complète initiale
    config              Reconfigure les variables et connexions
    fix                 Corrige la configuration (avec attente API)
    auto-fix            Correction automatique complète (détecte et corrige)
    verify              Vérifie la configuration actuelle
    verify-email        Vérifie le correctif email Airflow 3.x
    export              Exporte la configuration actuelle
    diagnose            Diagnostic complet du système
    test-config         Test rapide de la configuration

  AIRFLOW
    dags                Liste tous les DAGs
    trigger [dag_id]    Déclenche un DAG manuellement
    pause [dag_id]      Met en pause un DAG
    unpause [dag_id]    Réactive un DAG
    variables           Liste les variables Airflow
    connections         Liste les connexions Airflow
    add-table           Ajoute une table à importer
    list-tables         Liste les tables configurées
    remove-table [name] Supprime une table de la configuration

  BASE DE DONNÉES
    db-shell            Connexion au shell PostgreSQL
    db-backup           Sauvegarde la base de données
    db-restore [file]   Restaure une sauvegarde

  DÉVELOPPEMENT
    test [dag_id]       Test un DAG
    test-email          Test la configuration email
    shell               Shell interactif dans le container
    python              Console Python interactive
    clean               Nettoie les fichiers temporaires

  AUTRES
    help                Affiche cette aide
    version             Affiche les versions

Exemples:
  ./manage.sh start
  ./manage.sh logs airflow-scheduler
  ./manage.sh trigger amue_multi_table_import
  ./manage.sh config
  ./manage.sh test-email

Interfaces:
  - Airflow UI : http://localhost:8080 (airflow/airflow)
  - MailHog UI : http://localhost:8025 (voir les emails)

EOF
}

cmd_start() {
    log_info "Démarrage des services..."
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
    chmod +x scripts/install/quick_setup.sh
    ./scripts/install/quick_setup.sh
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

cmd_verify_email() {
    log_info "Vérification du correctif email Airflow 3.x..."
    chmod +x scripts/dev/verify_email_fix.sh
    ./scripts/dev/verify_email_fix.sh
}

cmd_diagnose() {
    log_info "Diagnostic complet du système..."
    chmod +x scripts/manage/diagnose.sh
    ./scripts/manage/diagnose.sh
}

cmd_test_config() {
    log_info "Test rapide de la configuration..."
    chmod +x scripts/quick_test.sh
    ./scripts/quick_test.sh
}

cmd_export() {
    log_info "Export de la configuration..."
    chmod +x scripts/install/setup_airflow_config.sh
    ./scripts/install/setup_airflow_config.sh --export
}

cmd_dags() {
    log_info "Liste des DAGs:"
    $DOCKER_CMD exec -T airflow-apiserver airflow dags list
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
    $DOCKER_CMD exec -T airflow-apiserver airflow dags trigger "$dag_id"
    log_success "DAG déclenché"
}

cmd_pause() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh pause <dag_id>"
        exit 1
    fi

    log_info "Mise en pause du DAG $dag_id..."
    $DOCKER_CMD exec -T airflow-apiserver airflow dags pause "$dag_id"
    log_success "DAG mis en pause"
}

cmd_unpause() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh unpause <dag_id>"
        exit 1
    fi

    log_info "Réactivation du DAG $dag_id..."
    $DOCKER_CMD exec -T airflow-apiserver airflow dags unpause "$dag_id"
    log_success "DAG réactivé"
}

cmd_variables() {
    log_info "Variables Airflow:"
    $DOCKER_CMD exec -T airflow-apiserver airflow variables list
}

cmd_connections() {
    log_info "Connexions Airflow:"
    $DOCKER_CMD exec -T airflow-apiserver airflow connections list
}

cmd_add_table() {
    log_info "Ajout d'une table à importer"
    echo ""

    # Demande le nom de la table
    echo -n "Nom de la table : " >&2
    read -r TABLE_NAME </dev/tty

    if [[ -z "$TABLE_NAME" ]]; then
        log_error "Le nom de la table est obligatoire"
        exit 1
    fi

    # Convertit en majuscules
    TABLE_NAME=$(echo "$TABLE_NAME" | tr '[:lower:]' '[:upper:]')

    # Demande la colonne delta (optionnelle)
    echo -n "Colonne delta pour import différentiel (optionnel) : " >&2
    read -r DELTA_COLUMN </dev/tty

    # Vérifie que jq est disponible
    if ! command -v jq &> /dev/null; then
        log_error "jq est requis pour cette opération"
        log_info "Installation: sudo apt-get install jq"
        exit 1
    fi

    log_info "Ajout de la table $TABLE_NAME..."

    # Récupère la configuration actuelle
    CURRENT_CONFIG=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_tables_to_import 2>/dev/null || echo "[]")

    # Vérifie si la table existe déjà avec jq
    TABLE_EXISTS=$(echo "$CURRENT_CONFIG" | jq -r "[.[] | select(.name == \"$TABLE_NAME\")] | length")

    if [[ "$TABLE_EXISTS" != "0" ]]; then
        log_warning "La table $TABLE_NAME existe déjà dans la configuration"
        exit 1
    fi

    # Ajoute la table à la configuration avec jq
    NEW_CONFIG=$(echo "$CURRENT_CONFIG" | jq -c \
        --arg name "$TABLE_NAME" \
        --arg delta "$DELTA_COLUMN" \
        '. + [{name: $name, primary_key: "", delta: $delta, last_import: "", finger_print: ""}]')

    # Met à jour la variable
    $DOCKER_CMD exec -T airflow-apiserver airflow variables set amue_tables_to_import "$NEW_CONFIG"

    log_success "Table $TABLE_NAME ajoutée avec succès"
    echo ""
    cmd_list_tables
}

cmd_list_tables() {
    log_info "Tables configurées pour l'import:"
    echo ""

    TABLES_CONFIG=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_tables_to_import 2>/dev/null || echo "[]")

    if command -v jq &> /dev/null; then
        echo "$TABLES_CONFIG" | jq -r '.[] | "  - \(.name)\(if .delta != "" then " (delta: \(.delta))" else "" end)\(if .last_import != "" then " [dernier import: \(.last_import)]" else "" end)"'
    else
        echo "$TABLES_CONFIG"
    fi
    echo ""
}

cmd_remove_table() {
    local table_name=${1:-}

    if [[ -z "$table_name" ]]; then
        log_error "Usage: ./manage.sh remove-table <nom_table>"
        echo ""
        cmd_list_tables
        exit 1
    fi

    # Vérifie que jq est disponible
    if ! command -v jq &> /dev/null; then
        log_error "jq est requis pour cette opération"
        log_info "Installation: sudo apt-get install jq"
        exit 1
    fi

    # Convertit en majuscules
    table_name=$(echo "$table_name" | tr '[:lower:]' '[:upper:]')

    log_info "Suppression de la table $table_name..."

    # Récupère la configuration actuelle
    CURRENT_CONFIG=$($DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_tables_to_import 2>/dev/null || echo "[]")

    # Vérifie si la table existe avec jq
    TABLE_EXISTS=$(echo "$CURRENT_CONFIG" | jq -r "[.[] | select(.name == \"$table_name\")] | length")

    if [[ "$TABLE_EXISTS" == "0" ]]; then
        log_warning "La table $table_name n'existe pas dans la configuration"
        echo ""
        log_info "Tables disponibles:"
        cmd_list_tables
        exit 1
    fi

    # Supprime la table
    NEW_CONFIG=$(echo "$CURRENT_CONFIG" | jq -c "[.[] | select(.name != \"$table_name\")]")
    $DOCKER_CMD exec -T airflow-apiserver airflow variables set amue_tables_to_import "$NEW_CONFIG"

    log_success "Table $table_name supprimée"
    echo ""
    cmd_list_tables
}

cmd_db_shell() {
    log_info "Connexion au shell PostgreSQL (base métier)..."
    $DOCKER_CMD exec -it postgres-data psql -U datauser -d business_data
}

cmd_db_backup() {
    local backup_dir="backups"
    mkdir -p "$backup_dir"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="${backup_dir}/business_data_${timestamp}.sql"

    log_info "Sauvegarde de la base de données..."
    $DOCKER_CMD exec -T postgres-data pg_dump -U datauser business_data > "$backup_file"
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
    read -p "Continuer? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Opération annulée"
        exit 0
    fi

    log_info "Restauration de la base de données..."
    $DOCKER_CMD exec -T postgres-data psql -U datauser -d business_data < "$backup_file"
    log_success "Base de données restaurée"
}

cmd_test() {
    local dag_id=${1:-}
    if [[ -z "$dag_id" ]]; then
        log_error "Usage: ./manage.sh test <dag_id>"
        exit 1
    fi

    log_info "Test du DAG $dag_id..."
    $DOCKER_CMD exec -T airflow-apiserver airflow dags test "$dag_id"
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
    log_info "Versions:"
    echo ""

    echo -n "Docker: "
    docker --version

    echo -n "Docker Compose: "
    $DOCKER_CMD version --short 2>/dev/null || echo "N/A"

    if $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
        echo -n "Airflow: "
        $DOCKER_CMD exec -T airflow-apiserver airflow version 2>/dev/null || echo "N/A"

        echo -n "Python: "
        $DOCKER_CMD exec -T airflow-apiserver python --version 2>/dev/null || echo "N/A"
    else
        log_warning "Services Airflow non démarrés"
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
        start)          cmd_start "$@" ;;
        stop)           cmd_stop "$@" ;;
        restart)        cmd_restart "$@" ;;
        status)         cmd_status "$@" ;;
        logs)           cmd_logs "$@" ;;

        setup)          cmd_setup "$@" ;;
        config)         cmd_config "$@" ;;
        fix)            cmd_fix "$@" ;;
        auto-fix)       cmd_auto_fix "$@" ;;
        verify)         cmd_verify "$@" ;;
        verify-email)   cmd_verify_email "$@" ;;
        export)         cmd_export "$@" ;;
        diagnose)       cmd_diagnose "$@" ;;
        test-config)    cmd_test_config "$@" ;;

        dags)           cmd_dags "$@" ;;
        trigger)        cmd_trigger "$@" ;;
        pause)          cmd_pause "$@" ;;
        unpause)        cmd_unpause "$@" ;;
        variables)      cmd_variables "$@" ;;
        connections)    cmd_connections "$@" ;;
        add-table)      cmd_add_table "$@" ;;
        list-tables)    cmd_list_tables "$@" ;;
        remove-table)   cmd_remove_table "$@" ;;

        db-shell)       cmd_db_shell "$@" ;;
        db-backup)      cmd_db_backup "$@" ;;
        db-restore)     cmd_db_restore "$@" ;;

        test)           cmd_test "$@" ;;
        test-email)     cmd_test_email "$@" ;;
        shell)          cmd_shell "$@" ;;
        python)         cmd_python "$@" ;;
        clean)          cmd_clean "$@" ;;

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