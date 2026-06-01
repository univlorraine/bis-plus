#!/bin/bash

###############################################################################
# Script de configuration Airflow
# Configure les variables et connexions (structure) depuis des fichiers JSON
# Les credentials ne sont JAMAIS écrits sur disque : ils sont injectés
# directement dans Airflow DB lors du setup initial via quick_setup.sh.
# Utilisation: ./setup_airflow_config.sh [--internal|--external]
###############################################################################

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/colors.sh"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG_DIR="${PROJECT_DIR}/config"
VARIABLES_FILE="${CONFIG_DIR}/airflow_variables.json"
CONNECTIONS_FILE="${CONFIG_DIR}/airflow_connections.json"
ENV_FILE="${PROJECT_DIR}/.env"
MODE="${1:-internal}"

###############################################################################
# Fonctions utilitaires
###############################################################################

check_file() {
    local file=$1
    if [[ ! -f "$file" ]]; then
        log_error "Fichier non trouve: $file"
        exit 1
    fi
}

check_jq() {
    if ! command -v jq &> /dev/null; then
        log_error "jq n'est pas installe. Installation requise: sudo apt-get install jq"
        exit 1
    fi
}

# Charge les variables d'environnement depuis .env (parseur securise, sans execution de code)
load_env() {
    if [[ -f "$ENV_FILE" ]]; then
        log_info "Chargement des credentials depuis .env"
        while IFS='=' read -r key value; do
            # Ignorer les commentaires et les lignes vides
            [[ "$key" =~ ^[[:space:]]*# ]] && continue
            [[ -z "${key// }" ]] && continue
            # Nettoyer les espaces autour de la cle
            key="${key#"${key%%[![:space:]]*}"}"
            key="${key%"${key##*[![:space:]]}"}"
            # Supprimer les guillemets autour de la valeur
            value="${value%\"}"
            value="${value#\"}"
            value="${value%\'}"
            value="${value#\'}"
            [[ -n "$key" ]] && export "${key}=${value}"
        done < "$ENV_FILE"
    else
        log_warning "Fichier .env non trouve. Les credentials devront etre definis manuellement."
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

    log_success "Configuration terminee avec succes!"
}

setup_variables_internal() {
    local count=0

    # Passe unique sur toutes les variables — evite N+2 appels jq
    # Separateur SOH (0x01) : safe pour les valeurs Airflow
    while IFS=$'\t' read -r key value; do
        [[ -z "$key" ]] && continue
        if airflow variables set "$key" "$value" 2>/dev/null; then
            log_success "Variable creee: $key"
            count=$((count + 1))
        else
            log_warning "Echec creation variable: $key"
        fi
    done < <(jq -r \
        'to_entries[] | .key + "\t" + (.value | if type == "array" or type == "object" then tojson else tostring end)' \
        "$VARIABLES_FILE")

    log_info "Total: $count variables creees"
}

setup_connections_internal() {
    local count=0

    for conn_id in $(jq -r 'keys[]' "$CONNECTIONS_FILE"); do
        # Skip si la connexion existe deja (credentials preserves dans Airflow DB)
        if airflow connections get "$conn_id" > /dev/null 2>&1; then
            log_info "Connexion existante ignoree (credentials preserves): $conn_id"
            continue
        fi

        # Lecture de tous les champs en un seul appel jq (au lieu de 4 appels separes)
        mapfile -t _conn_fields < <(jq -r --arg id "$conn_id" \
            '.[$id] | .conn_type,
                      (.host // ""),
                      ((.port // "") | tostring),
                      (.schema // ""),
                      (.extra // {} | tojson)' \
            "$CONNECTIONS_FILE")
        local conn_type="${_conn_fields[0]}"
        local host="${_conn_fields[1]}"
        local port="${_conn_fields[2]}"
        local schema="${_conn_fields[3]}"
        local extra="${_conn_fields[4]}"

        local add_cmd=(airflow connections add "$conn_id" --conn-type "$conn_type")
        [[ -n "$host"   ]] && add_cmd+=(--conn-host   "$host")
        [[ -n "$port"   ]] && add_cmd+=(--conn-port   "$port")
        [[ -n "$schema" ]] && add_cmd+=(--conn-schema "$schema")
        [[ "$extra" != "{}" ]] && add_cmd+=(--conn-extra "$extra")

        if "${add_cmd[@]}" 2>/dev/null; then
            log_success "Connexion creee (structure): $conn_id ($conn_type)"
            count=$((count + 1))
        else
            log_warning "Echec creation connexion: $conn_id"
        fi
    done

    log_info "Total: $count connexions creees"
}

###############################################################################
# Configuration en mode externe (via docker-compose exec)
###############################################################################

setup_external() {
    log_info "Mode externe: Configuration via docker-compose"

    check_file "$VARIABLES_FILE"
    check_file "$CONNECTIONS_FILE"
    check_jq

    # Verifie que docker-compose est disponible
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        log_error "docker-compose ou docker non trouve"
        exit 1
    fi

    local docker_cmd="docker-compose"
    if ! command -v docker-compose &> /dev/null; then
        docker_cmd="docker compose"
    fi

    # Verifie que l'API Airflow est accessible
    if ! curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
        log_error "L'API Airflow n'est pas accessible sur http://localhost:8080"
        log_info "Demarrez Airflow avec: $docker_cmd up -d"
        exit 1
    fi

    log_info "Configuration des variables Airflow..."
    setup_variables_external "$docker_cmd"

    log_info "Configuration des connexions Airflow..."
    setup_connections_external "$docker_cmd"

    log_success "Configuration terminee avec succes!"
}

setup_variables_external() {
    local docker_cmd=$1

    log_info "Import des variables..."

    # Cree un fichier temporaire avec les valeurs complexes serialisees en JSON string
    local temp_file
    temp_file=$(mktemp --suffix=.json)
    jq 'to_entries | map({key: .key, value: (if (.value | type) == "array" or (.value | type) == "object" then (.value | tojson) else .value end)}) | from_entries' "$VARIABLES_FILE" > "$temp_file"

    # Copie le fichier dans le container et importe
    $docker_cmd cp "$temp_file" airflow-apiserver:/tmp/airflow_vars_import.json

    if $docker_cmd exec -T airflow-apiserver airflow variables import /tmp/airflow_vars_import.json 2>&1 | grep -v "^$"; then
        local count
        count=$(jq 'keys | length' "$VARIABLES_FILE")
        log_success "$count variables importees"
    else
        log_warning "Import echoue"
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
        # Skip si la connexion existe deja (credentials preserves dans Airflow DB)
        if $docker_cmd exec -T airflow-apiserver airflow connections get "$conn_id" \
                > /dev/null 2>&1; then
            log_info "Connexion existante ignoree (credentials preserves): $conn_id"
            continue
        fi

        # Lecture de tous les champs en un seul appel jq (au lieu de 4 appels separes)
        mapfile -t _conn_fields < <(jq -r --arg id "$conn_id" \
            '.[$id] | .conn_type,
                      (.host // ""),
                      ((.port // "") | tostring),
                      (.schema // ""),
                      (.extra // {} | tojson)' \
            "$CONNECTIONS_FILE")
        local conn_type="${_conn_fields[0]}"
        local host="${_conn_fields[1]}"
        local port="${_conn_fields[2]}"
        local schema="${_conn_fields[3]}"
        local extra="${_conn_fields[4]}"

        local add_cmd=(airflow connections add "$conn_id" --conn-type "$conn_type")
        [[ -n "$host"   ]] && add_cmd+=(--conn-host   "$host")
        [[ -n "$port"   ]] && add_cmd+=(--conn-port   "$port")
        [[ -n "$schema" ]] && add_cmd+=(--conn-schema "$schema")
        [[ "$extra" != "{}" ]] && add_cmd+=(--conn-extra "$extra")

        if $docker_cmd exec -T airflow-apiserver "${add_cmd[@]}" 2>/dev/null; then
            log_success "Connexion creee (structure): $conn_id"
            count=$((count + 1))
        else
            log_warning "Echec creation connexion: $conn_id"
        fi
    done

    log_info "Total: $count connexions creees"
}

###############################################################################
# Fonction de verification
###############################################################################

verify_configuration() {
    log_info "Verification de la configuration..."

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

    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
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

    log_success "Export termine dans: $export_dir"
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
et stockes directement dans la base de donnees Airflow (chiffres).

Options:
  --internal, -i       Configuration depuis l'interieur du container
  --external, -e       Configuration depuis l'exterieur via docker-compose
  --variables-only, -V Import des variables uniquement (sans toucher aux connexions)
                        Utilise par quick_setup.sh qui cree les connexions avec credentials
  --verify, -v         Verifie la configuration actuelle
  --export, -x         Exporte la configuration actuelle
  --help, -h           Affiche cette aide

Exemples:
  $0 --external              # Configure variables + connexions (structure seule) depuis l'hote
  $0 --internal              # Configure variables + connexions depuis le container
  $0 --variables-only        # Import des variables uniquement (connexions gerees par quick_setup.sh)
  $0 --verify                # Verifie la config
  $0 --export                # Exporte la config

Fichiers requis:
  - config/airflow_variables.json (sans secrets)
  - config/airflow_connections.json (structure uniquement, sans credentials)

Note: Les connexions avec credentials doivent etre creees via quick_setup.sh.
Les connexions existantes sont ignorees en mode --external/--internal (credentials preserves).

EOF
}

###############################################################################
# Point d'entree
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
            if ! curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
                log_error "L'API Airflow n'est pas accessible sur http://localhost:8080"
                exit 1
            fi
            log_info "Import des variables Airflow (connexions ignorees)..."
            setup_variables_external "$docker_cmd"
            log_success "Variables configurees. Les connexions sont gerees par quick_setup.sh."
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
