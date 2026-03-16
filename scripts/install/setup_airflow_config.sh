#!/bin/bash

###############################################################################
# Script de configuration Airflow
# Configure les variables et connexions (structure) depuis des fichiers JSON
# Les credentials ne sont JAMAIS écrits sur disque : ils sont injectés
# directement dans Airflow DB lors du setup initial via quick_setup.sh.
# Utilisation: ./setup_airflow_config.sh [--internal|--external]
###############################################################################

set -e

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration par défaut
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG_DIR="${PROJECT_DIR}/config"
VARIABLES_FILE="${CONFIG_DIR}/airflow_variables.json"
CONNECTIONS_FILE="${CONFIG_DIR}/airflow_connections.json"
ENV_FILE="${PROJECT_DIR}/.env"
MODE="${1:-internal}"

###############################################################################
# Fonctions utilitaires
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_file() {
    local file=$1
    if [[ ! -f "$file" ]]; then
        log_error "Fichier non trouvé: $file"
        exit 1
    fi
}

check_jq() {
    if ! command -v jq &> /dev/null; then
        log_error "jq n'est pas installé. Installation requise: sudo apt-get install jq"
        exit 1
    fi
}

# Charge les variables d'environnement depuis .env
load_env() {
    if [[ -f "$ENV_FILE" ]]; then
        log_info "Chargement des credentials depuis .env"
        set -a
        source "$ENV_FILE"
        set +a
    else
        log_warning "Fichier .env non trouvé. Les credentials devront être définis manuellement."
    fi
}

###############################################################################
# Configuration en mode interne (dans le container)
###############################################################################

setup_internal() {
    log_info "Mode interne: Configuration depuis le container Airflow"

    check_file "$VARIABLES_FILE"
    check_file "$CONNECTIONS_FILE"
    check_jq

    log_info "Configuration des variables Airflow..."
    setup_variables_internal

    log_info "Configuration des connexions Airflow..."
    setup_connections_internal

    log_success "Configuration terminée avec succès!"
}

setup_variables_internal() {
    local count=0

    # Variables simples (string/number)
    for key in $(jq -r 'to_entries | map(select(.value | type != "array" and type != "object")) | .[].key' "$VARIABLES_FILE"); do
        local value=$(jq -r ".[\"$key\"]" "$VARIABLES_FILE")

        if airflow variables set "$key" "$value" 2>/dev/null; then
            log_success "Variable créée: $key"
            count=$((count + 1))
        else
            log_warning "Échec création variable: $key"
        fi
    done

    # Variables complexes (arrays/objects) - en JSON
    for key in $(jq -r 'to_entries | map(select(.value | type == "array" or type == "object")) | .[].key' "$VARIABLES_FILE"); do
        local value=$(jq -c ".[\"$key\"]" "$VARIABLES_FILE")

        if airflow variables set "$key" "$value" 2>/dev/null; then
            log_success "Variable créée: $key (JSON)"
            count=$((count + 1))
        else
            log_warning "Échec création variable: $key"
        fi
    done

    log_info "Total: $count variables créées"
}

setup_connections_internal() {
    local count=0

    for conn_id in $(jq -r 'keys[]' "$CONNECTIONS_FILE"); do
        # Skip si la connexion existe déjà (credentials préservés dans Airflow DB)
        if airflow connections get "$conn_id" > /dev/null 2>&1; then
            log_info "Connexion existante ignorée (credentials préservés): $conn_id"
            continue
        fi

        local conn_type=$(jq -r ".[\"$conn_id\"].conn_type" "$CONNECTIONS_FILE")
        local host=$(jq -r ".[\"$conn_id\"].host // empty" "$CONNECTIONS_FILE")
        local port=$(jq -r ".[\"$conn_id\"].port // empty" "$CONNECTIONS_FILE")
        local schema=$(jq -r ".[\"$conn_id\"].schema // empty" "$CONNECTIONS_FILE")
        local extra=$(jq -c ".[\"$conn_id\"].extra // {}" "$CONNECTIONS_FILE")

        # Construction de la commande (champs structurels uniquement, sans credentials)
        local cmd="airflow connections add '$conn_id' --conn-type '$conn_type'"

        [[ -n "$host" ]]   && cmd="$cmd --conn-host '$host'"
        [[ -n "$port" ]]   && cmd="$cmd --conn-port '$port'"
        [[ -n "$schema" ]] && cmd="$cmd --conn-schema '$schema'"
        [[ "$extra" != "{}" ]] && cmd="$cmd --conn-extra '$extra'"

        if eval "$cmd" 2>/dev/null; then
            log_success "Connexion créée (structure): $conn_id ($conn_type)"
            count=$((count + 1))
        else
            log_warning "Échec création connexion: $conn_id"
        fi
    done

    log_info "Total: $count connexions créées"
}

###############################################################################
# Configuration en mode externe (via docker-compose exec)
###############################################################################

setup_external() {
    log_info "Mode externe: Configuration via docker-compose"

    check_file "$VARIABLES_FILE"
    check_file "$CONNECTIONS_FILE"
    check_jq

    # Vérifie que docker-compose est disponible
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        log_error "docker-compose ou docker non trouvé"
        exit 1
    fi

    local docker_cmd="docker-compose"
    if ! command -v docker-compose &> /dev/null; then
        docker_cmd="docker compose"
    fi

    # Vérifie que le service airflow-apiserver est en cours d'exécution
    if ! $docker_cmd ps | grep -q "airflow-apiserver"; then
        log_error "Le service airflow-apiserver n'est pas en cours d'exécution"
        log_info "Démarrez Airflow avec: $docker_cmd up -d"
        exit 1
    fi

    log_info "Configuration des variables Airflow..."
    setup_variables_external "$docker_cmd"

    log_info "Configuration des connexions Airflow..."
    setup_connections_external "$docker_cmd"

    log_success "Configuration terminée avec succès!"
}

setup_variables_external() {
    local docker_cmd=$1

    log_info "Import des variables..."

    # Crée un fichier temporaire avec les valeurs complexes sérialisées en JSON string
    local temp_file="/tmp/airflow_vars_import.json"
    jq 'to_entries | map({key: .key, value: (if (.value | type) == "array" or (.value | type) == "object" then (.value | tojson) else .value end)}) | from_entries' "$VARIABLES_FILE" > "$temp_file"

    # Copie le fichier dans le container et importe
    $docker_cmd cp "$temp_file" airflow-apiserver:/tmp/airflow_vars_import.json

    if $docker_cmd exec -T airflow-apiserver airflow variables import /tmp/airflow_vars_import.json 2>&1 | grep -v "^$"; then
        local count=$(jq 'keys | length' "$VARIABLES_FILE")
        log_success "$count variables importées"
    else
        log_warning "Import échoué"
    fi

    # Nettoyage
    rm -f "$temp_file"
    $docker_cmd exec -T airflow-apiserver rm -f /tmp/airflow_vars_import.json 2>/dev/null || true
}

setup_connections_external() {
    local docker_cmd=$1

    log_info "Import des connexions (structure uniquement, sans credentials)..."

    local count=0

    for conn_id in $(jq -r 'keys[]' "$CONNECTIONS_FILE"); do
        # Skip si la connexion existe déjà (credentials préservés dans Airflow DB)
        if $docker_cmd exec -T airflow-apiserver airflow connections get "$conn_id" \
                > /dev/null 2>&1; then
            log_info "Connexion existante ignorée (credentials préservés): $conn_id"
            continue
        fi

        local conn_type host port schema extra cmd
        conn_type=$(jq -r ".[\"$conn_id\"].conn_type" "$CONNECTIONS_FILE")
        host=$(jq -r ".[\"$conn_id\"].host // empty" "$CONNECTIONS_FILE")
        port=$(jq -r ".[\"$conn_id\"].port // empty" "$CONNECTIONS_FILE")
        schema=$(jq -r ".[\"$conn_id\"].schema // empty" "$CONNECTIONS_FILE")
        extra=$(jq -c ".[\"$conn_id\"].extra // {}" "$CONNECTIONS_FILE")

        cmd="airflow connections add '$conn_id' --conn-type '$conn_type'"
        [[ -n "$host" ]]   && cmd="$cmd --conn-host '$host'"
        [[ -n "$port" ]]   && cmd="$cmd --conn-port '$port'"
        [[ -n "$schema" ]] && cmd="$cmd --conn-schema '$schema'"
        [[ "$extra" != "{}" ]] && cmd="$cmd --conn-extra '$extra'"

        if $docker_cmd exec -T airflow-apiserver bash -c "$cmd" 2>/dev/null; then
            log_success "Connexion créée (structure): $conn_id"
            count=$((count + 1))
        else
            log_warning "Échec création connexion: $conn_id"
        fi
    done

    log_info "Total: $count connexions créées"
}

###############################################################################
# Fonction de vérification
###############################################################################

verify_configuration() {
    log_info "Vérification de la configuration..."

    local docker_cmd="docker-compose"
    if ! command -v docker-compose &> /dev/null; then
        docker_cmd="docker compose"
    fi

    echo ""
    log_info "=== Variables Airflow ==="
    $docker_cmd exec -T airflow-apiserver airflow variables list

    echo ""
    log_info "=== Connexions Airflow ==="
    $docker_cmd exec -T airflow-apiserver airflow connections list
}

###############################################################################
# Fonction d'export
###############################################################################

export_configuration() {
    log_info "Export de la configuration actuelle..."

    local docker_cmd="docker-compose"
    if ! command -v docker-compose &> /dev/null; then
        docker_cmd="docker compose"
    fi

    local export_dir="${CONFIG_DIR}/exports"
    mkdir -p "$export_dir"

    local timestamp=$(date +%Y%m%d_%H%M%S)
    local vars_export="${export_dir}/variables_${timestamp}.json"
    local conns_export="${export_dir}/connections_${timestamp}.txt"

    # Export des variables
    $docker_cmd exec -T airflow-apiserver airflow variables export "$vars_export" 2>/dev/null || {
        log_warning "Impossible d'exporter les variables"
    }

    # Export des connexions (format texte)
    $docker_cmd exec -T airflow-apiserver airflow connections export "$conns_export" 2>/dev/null || {
        log_warning "Impossible d'exporter les connexions"
    }

    log_success "Export terminé dans: $export_dir"
    log_warning "ATTENTION: L'export des connexions peut contenir des credentials!"
}

###############################################################################
# Menu principal
###############################################################################

detect_docker_cmd() {
    if command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    else
        echo "docker compose"
    fi
}

show_usage() {
    cat << EOF
Usage: $0 [OPTION]

Configure les variables et connexions Airflow depuis des fichiers JSON.
Les credentials (login/password) sont saisis lors du setup initial (quick_setup.sh)
et stockés directement dans la base de données Airflow (chiffrés).

Options:
  --internal, -i       Configuration depuis l'intérieur du container
  --external, -e       Configuration depuis l'extérieur via docker-compose
  --variables-only, -V Import des variables uniquement (sans toucher aux connexions)
                        Utilisé par quick_setup.sh qui crée les connexions avec credentials
  --verify, -v         Vérifie la configuration actuelle
  --export, -x         Exporte la configuration actuelle
  --help, -h           Affiche cette aide

Exemples:
  $0 --external              # Configure variables + connexions (structure seule) depuis l'hôte
  $0 --internal              # Configure variables + connexions depuis le container
  $0 --variables-only        # Import des variables uniquement (connexions gérées par quick_setup.sh)
  $0 --verify                # Vérifie la config
  $0 --export                # Exporte la config

Fichiers requis:
  - config/airflow_variables.json (sans secrets)
  - config/airflow_connections.json (structure uniquement, sans credentials) — requis sauf en mode --variables-only

Note: Les connexions avec credentials doivent être créées via quick_setup.sh.
Les connexions existantes sont ignorées en mode --external/--internal (credentials préservés).

EOF
}

###############################################################################
# Point d'entrée
###############################################################################

main() {
    case "${1:-}" in
        --internal|-i)
            setup_internal
            ;;
        --external|-e)
            setup_external
            ;;
        --variables-only|-V)
            local docker_cmd
            docker_cmd="$(detect_docker_cmd)"
            check_file "$VARIABLES_FILE"
            check_jq
            if ! $docker_cmd ps | grep -q "airflow-apiserver"; then
                log_error "Le service airflow-apiserver n'est pas en cours d'exécution"
                exit 1
            fi
            log_info "Import des variables Airflow (connexions ignorées)..."
            setup_variables_external "$docker_cmd"
            log_success "Variables configurées. Les connexions sont gérées par quick_setup.sh."
            ;;
        --verify|-v)
            verify_configuration
            ;;
        --export|-x)
            export_configuration
            ;;
        --help|-h)
            show_usage
            exit 0
            ;;
        *)
            log_error "Option invalide: ${1:-}"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

main "$@"
