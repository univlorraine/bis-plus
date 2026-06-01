#!/bin/bash
###############################################################################
# scripts/lib/docker.sh
# Fonctions utilitaires Docker
###############################################################################

# Source les couleurs si pas déjà fait
SCRIPT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -z "$NC" ]] && source "$SCRIPT_LIB_DIR/colors.sh"

###############################################################################
# Gestion des chemins du projet
###############################################################################

# Initialise les chemins du projet
# Appelé automatiquement, peut être surchargé via variables d'environnement
init_project_paths() {
    # Chemin du projet (où sont dags, plugins, config)
    if [[ -z "$AIRFLOW_PROJ_DIR" ]]; then
        # Détection basée sur l'emplacement du script appelant
        local caller_dir="$(cd "$(dirname "${BASH_SOURCE[2]:-${BASH_SOURCE[1]}}")" && pwd)"
        # Remonter si on est dans scripts/
        if [[ "$caller_dir" == */scripts/* ]]; then
            AIRFLOW_PROJ_DIR="$(cd "$caller_dir/../.." && pwd)"
        elif [[ "$caller_dir" == */scripts ]]; then
            AIRFLOW_PROJ_DIR="$(cd "$caller_dir/.." && pwd)"
        else
            AIRFLOW_PROJ_DIR="$caller_dir"
        fi
    fi
    export AIRFLOW_PROJ_DIR

    # Chemin du docker-compose (peut être différent en prod)
    export DOCKER_COMPOSE_DIR="${DOCKER_COMPOSE_DIR:-$AIRFLOW_PROJ_DIR}"

    # Chemin du fichier .env
    export ENV_FILE_PATH="${ENV_FILE_PATH:-$AIRFLOW_PROJ_DIR/.env}"
}

# Retourne la commande docker-compose avec les options appropriées
get_docker_compose_cmd() {
    local base_cmd
    if command -v docker-compose &> /dev/null; then
        base_cmd="docker-compose"
    else
        base_cmd="docker compose"
    fi

    local opts=""

    # -f si docker-compose.yml est dans un autre dossier
    if [[ -n "$DOCKER_COMPOSE_DIR" ]] && [[ "$DOCKER_COMPOSE_DIR" != "$AIRFLOW_PROJ_DIR" ]]; then
        opts="$opts -f $DOCKER_COMPOSE_DIR/docker-compose.yml"
    fi

    # --env-file si .env est spécifié explicitement
    if [[ -n "$ENV_FILE_PATH" ]] && [[ "$ENV_FILE_PATH" != ".env" ]] && [[ -f "$ENV_FILE_PATH" ]]; then
        opts="$opts --env-file $ENV_FILE_PATH"
    fi

    echo "$base_cmd$opts"
}

# Vérifie que les chemins sont valides
verify_project_paths() {
    local errors=0

    echo "Configuration des chemins:"
    echo "  AIRFLOW_PROJ_DIR:   ${AIRFLOW_PROJ_DIR:-<non défini>}"
    echo "  DOCKER_COMPOSE_DIR: ${DOCKER_COMPOSE_DIR:-<non défini>}"
    echo "  ENV_FILE_PATH:      ${ENV_FILE_PATH:-<non défini>}"
    echo ""

    [[ ! -d "$AIRFLOW_PROJ_DIR" ]] && { log_error "AIRFLOW_PROJ_DIR n'existe pas: $AIRFLOW_PROJ_DIR"; ((errors++)); }
    [[ ! -d "$AIRFLOW_PROJ_DIR/dags" ]] && { log_warning "Dossier dags manquant: $AIRFLOW_PROJ_DIR/dags"; }
    [[ ! -d "$AIRFLOW_PROJ_DIR/plugins" ]] && { log_warning "Dossier plugins manquant: $AIRFLOW_PROJ_DIR/plugins"; }
    [[ ! -f "$DOCKER_COMPOSE_DIR/docker-compose.yml" ]] && { log_error "docker-compose.yml non trouvé: $DOCKER_COMPOSE_DIR/docker-compose.yml"; ((errors++)); }
    [[ -n "$ENV_FILE_PATH" ]] && [[ ! -f "$ENV_FILE_PATH" ]] && { log_warning "Fichier .env non trouvé: $ENV_FILE_PATH"; }

    return $errors
}

###############################################################################
# Détection Docker Compose
###############################################################################

# Détecte la commande docker-compose disponible (rétrocompatibilité)
detect_docker_compose() {
    # Initialise les chemins si pas fait
    [[ -z "$AIRFLOW_PROJ_DIR" ]] && init_project_paths
    get_docker_compose_cmd
}

# Vérifie que Docker est disponible
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker n'est pas installé"
        return 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Le daemon Docker n'est pas accessible"
        return 1
    fi

    return 0
}

# Vérifie qu'un service Docker est en cours d'exécution
check_service() {
    local service=$1
    local docker_cmd="${2:-$(detect_docker_compose)}"

    local status
    status=$($docker_cmd ps --filter "name=$service" --format "{{.Status}}" 2>/dev/null | head -1)
    if [[ -z "$status" ]]; then
        log_fail "$service n'existe pas"
        return 1
    elif echo "$status" | grep -q "^Up"; then
        log_ok "$service est en cours d'exécution"
        return 0
    else
        log_fail "$service n'est pas en état 'Up'"
        return 1
    fi
}

# Attend qu'un service soit prêt (healthcheck)
wait_for_service() {
    local service=$1
    local max_wait=${2:-60}
    local docker_cmd="${3:-$(detect_docker_compose)}"
    local elapsed=0
    local _sleep=1

    log_info "Attente du service $service..."

    while [[ $elapsed -lt $max_wait ]]; do
        if $docker_cmd ps --filter "name=$service" --format "{{.Status}}" 2>/dev/null | grep -q "healthy"; then
            log_success "$service est prêt"
            return 0
        fi
        sleep "$_sleep"
        elapsed=$(( elapsed + _sleep ))
        [[ $_sleep -lt 5 ]] && _sleep=$(( _sleep * 2 )) || _sleep=5
    done

    log_error "$service n'est pas prêt après ${max_wait}s"
    return 1
}

# Récupère les logs d'un service
get_service_logs() {
    local service=$1
    local lines=${2:-50}
    local docker_cmd="${3:-$(detect_docker_compose)}"

    $docker_cmd logs --tail "$lines" "$service" 2>&1
}

# Redémarre un service
restart_service() {
    local service=$1
    local docker_cmd="${2:-$(detect_docker_compose)}"

    log_info "Redémarrage de $service..."
    if $docker_cmd restart "$service" 2>&1; then
        log_success "$service redémarré"
        return 0
    else
        log_error "Échec du redémarrage de $service"
        return 1
    fi
}

# Exécute une commande dans un container
exec_in_container() {
    local container=$1
    shift
    local docker_cmd
    docker_cmd=$(detect_docker_compose)
    $docker_cmd exec -T "$container" "$@"
}
