#!/bin/bash
###############################################################################
# scripts/lib/airflow.sh
# Fonctions utilitaires Airflow
###############################################################################

# Source les dépendances
SCRIPT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -z "$NC" ]] && source "$SCRIPT_LIB_DIR/colors.sh"
[[ -z "$DOCKER_CMD" ]] && source "$SCRIPT_LIB_DIR/docker.sh"

# Container Airflow par défaut
AIRFLOW_CONTAINER="${AIRFLOW_CONTAINER:-airflow-apiserver}"
AIRFLOW_API_URL="${AIRFLOW_API_URL:-http://localhost:8080}"

# Vérifie que l'API Airflow est accessible
check_airflow_api() {
    local url="${1:-$AIRFLOW_API_URL}"
    local timeout="${2:-5}"

    if curl -s --max-time "$timeout" -o /dev/null -w "%{http_code}" "$url/api/v2/version" | grep -q "200"; then
        log_ok "API Airflow est accessible"
        return 0
    else
        log_fail "API Airflow n'est pas accessible sur $url"
        return 1
    fi
}

# Récupère la version Airflow
get_airflow_version() {
    local url="${1:-$AIRFLOW_API_URL}"

    curl -s "$url/api/v2/version" 2>/dev/null | jq -r '.version' 2>/dev/null || echo "N/A"
}

# Vérifie une variable Airflow
check_airflow_variable() {
    local var_name="$1"
    local docker_cmd="${2:-$(detect_docker_compose)}"

    local result
    result=$($docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow variables get "$var_name" 2>/dev/null)

    if [[ -n "$result" && "$result" != "null" ]]; then
        log_ok "Variable '$var_name' est définie"
        return 0
    else
        log_warn "Variable '$var_name' n'est pas définie"
        return 1
    fi
}

# Définit une variable Airflow
set_airflow_variable() {
    local var_name="$1"
    local var_value="$2"
    local docker_cmd="${3:-$(detect_docker_compose)}"

    if $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow variables set "$var_name" "$var_value" 2>/dev/null; then
        log_ok "Variable '$var_name' définie"
        return 0
    else
        log_fail "Échec de la définition de '$var_name'"
        return 1
    fi
}

# Vérifie une connexion Airflow
check_airflow_connection() {
    local conn_id="$1"
    local docker_cmd="${2:-$(detect_docker_compose)}"

    if $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow connections get "$conn_id" &>/dev/null; then
        log_ok "Connexion '$conn_id' existe"
        return 0
    else
        log_warn "Connexion '$conn_id' n'existe pas"
        return 1
    fi
}

# Crée ou met à jour une connexion Airflow
set_airflow_connection() {
    local conn_id="$1"
    local conn_type="$2"
    local conn_uri="$3"
    local docker_cmd="${4:-$(detect_docker_compose)}"

    # Supprime si existe déjà
    $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow connections delete "$conn_id" 2>/dev/null || true

    if $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow connections add "$conn_id" \
        --conn-type "$conn_type" \
        --conn-uri "$conn_uri" 2>/dev/null; then
        log_ok "Connexion '$conn_id' créée"
        return 0
    else
        log_fail "Échec de la création de '$conn_id'"
        return 1
    fi
}

# Liste les DAGs
list_dags() {
    local docker_cmd="${1:-$(detect_docker_compose)}"

    $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow dags list 2>/dev/null
}

# Vérifie qu'un DAG existe et est activé
check_dag() {
    local dag_id="$1"
    local docker_cmd="${2:-$(detect_docker_compose)}"

    # Un seul appel JSON au lieu de deux (plain + json)
    local dag_status
    dag_status=$($docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow dags list --output json 2>/dev/null | \
        python3 -c "
import json, sys, os
d = json.load(sys.stdin)
target = os.environ.get('_CHECK_DAG_ID', '')
r = [x for x in (d if isinstance(d, list) else []) if x.get('dag_id') == target]
print(r[0].get('is_paused', 'missing') if r else 'missing')
" 2>/dev/null || echo "missing") _CHECK_DAG_ID="$dag_id"

    if [[ "$dag_status" == "False" ]] || [[ "$dag_status" == "false" ]]; then
        log_ok "DAG '$dag_id' existe et est activé"
        return 0
    elif [[ "$dag_status" == "missing" ]]; then
        log_fail "DAG '$dag_id' n'existe pas"
        return 2
    else
        log_warn "DAG '$dag_id' existe mais n'est pas activé"
        return 1
    fi
}

# Active un DAG
enable_dag() {
    local dag_id="$1"
    local docker_cmd="${2:-$(detect_docker_compose)}"

    if $docker_cmd exec -T "$AIRFLOW_CONTAINER" \
        airflow dags unpause "$dag_id" 2>/dev/null; then
        log_ok "DAG '$dag_id' activé"
        return 0
    else
        log_fail "Échec de l'activation de '$dag_id'"
        return 1
    fi
}
