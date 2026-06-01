#!/bin/bash

###############################################################################
# Script de correction de la configuration Airflow
# Attend que l'API soit prête puis configure les variables et connexions
###############################################################################

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

source "$SCRIPT_DIR/../lib/colors.sh"
CONFIG_DIR="${PROJECT_DIR}/config"
VARIABLES_FILE="${CONFIG_DIR}/airflow_variables.json"
CONNECTIONS_FILE="${CONFIG_DIR}/airflow_connections.json"

# Détecte docker-compose
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║               Correction Configuration Airflow                ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

###############################################################################
# 1. Vérifications préalables
###############################################################################

log_info "Étape 1/4: Vérifications"

# Vérifie que les services sont démarrés
if ! curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
    log_error "L'API Airflow n'est pas accessible — démarrez avec: ./manage.sh start"
    exit 1
fi

# Vérifie que les fichiers existent
if [ ! -f "$VARIABLES_FILE" ]; then
    log_error "Fichier manquant: $VARIABLES_FILE"
    exit 1
fi

if [ ! -f "$CONNECTIONS_FILE" ]; then
    log_error "Fichier manquant: $CONNECTIONS_FILE"
    exit 1
fi

# Vérifie que jq est installé
if ! command -v jq &> /dev/null; then
    log_error "jq n'est pas installé"
    exit 1
fi

# Vérifie la validité JSON
if ! jq empty "$VARIABLES_FILE" 2>/dev/null; then
    log_error "Fichier JSON invalide: $VARIABLES_FILE"
    exit 1
fi

if ! jq empty "$CONNECTIONS_FILE" 2>/dev/null; then
    log_error "Fichier JSON invalide: $CONNECTIONS_FILE"
    exit 1
fi

log_success "Vérifications OK"

###############################################################################
# 2. Attente de l'API Airflow
###############################################################################

log_info "Étape 2/4: Attente de l'API Airflow"

MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/version 2>/dev/null | grep -q "200"; then
        log_success "API Airflow prête"
        break
    fi

    if [ $ATTEMPT -eq 0 ]; then
        log_info "En attente de l'API Airflow..."
    fi

    ATTEMPT=$((ATTEMPT + 1))

    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        log_error "Timeout: API Airflow non disponible après $MAX_ATTEMPTS tentatives"
        log_info "Vérifiez les logs: ./manage.sh logs airflow-apiserver"
        exit 1
    fi

    sleep 2
done


###############################################################################
# 3. Configuration des Variables
###############################################################################

log_info "Étape 3/4: Configuration des Variables"

VARS_SUCCESS=0
VARS_FAILED=0

# Passe unique sur toutes les variables (simples et JSON) — evite N+2 appels jq
# Separateur SOH (0x01) : safe pour les valeurs Airflow, compatible Linux/Windows
while IFS=$'\001' read -r key value; do
    [[ -z "$key" ]] && continue
    log_info "Configuration de la variable: $key"
    log_info "  Valeur: $value"

    _log=$(mktemp)
    if $DOCKER_CMD exec -T airflow-apiserver airflow variables set "$key" "$value" 2>&1 | tee "$_log" | grep -qE "Variable|success|created|updated|set"; then
        log_success "  Variable '$key' creee"
        VARS_SUCCESS=$((VARS_SUCCESS + 1))
    else
        log_error "  Echec pour '$key'"
        cat "$_log"
        VARS_FAILED=$((VARS_FAILED + 1))
    fi
    rm -f "$_log"
done < <(jq -r \
    'to_entries[] | .key + "" + (.value | if type == "array" or type == "object" then tojson else tostring end)' \
    "$VARIABLES_FILE")

echo ""
log_info "Variables: $VARS_SUCCESS reussies, $VARS_FAILED echecs"

###############################################################################
# 4. Configuration des Connexions
###############################################################################

log_info "Étape 4/4: Configuration des Connexions"

CONNS_SUCCESS=0
CONNS_FAILED=0

for conn_id in $(jq -r 'keys[]' "$CONNECTIONS_FILE"); do
    log_info "Configuration de la connexion: $conn_id"

    # Lecture de tous les champs en un seul appel jq (au lieu de 6 appels separes)
    mapfile -t _conn_fields < <(jq -r --arg id "$conn_id" \
        '.[$id] | .conn_type,
                  (.host // ""),
                  (.login // ""),
                  (.password // ""),
                  ((.port // "") | tostring),
                  (.schema // ""),
                  (.extra // {} | tojson)' \
        "$CONNECTIONS_FILE")
    conn_type="${_conn_fields[0]}"
    host="${_conn_fields[1]}"
    login="${_conn_fields[2]}"
    password="${_conn_fields[3]}"
    port="${_conn_fields[4]}"
    schema="${_conn_fields[5]}"
    extra="${_conn_fields[6]}"

    $DOCKER_CMD exec -T airflow-apiserver airflow connections delete "$conn_id" >/dev/null 2>&1 || true

    # Tableau bash — evite les problemes d'apostrophes dans les valeurs
    add_cmd=(airflow connections add "$conn_id" --conn-type "$conn_type")
    [ -n "$host"     ] && add_cmd+=(--conn-host     "$host")
    [ -n "$login"    ] && add_cmd+=(--conn-login    "$login")
    [ -n "$password" ] && add_cmd+=(--conn-password "$password")
    [ -n "$port"     ] && add_cmd+=(--conn-port     "$port")
    [ -n "$schema"   ] && add_cmd+=(--conn-schema   "$schema")
    [ "$extra" != "{}" ] && [ "$extra" != "null" ] && add_cmd+=(--conn-extra "$extra")

    _log=$(mktemp)
    if $DOCKER_CMD exec -T airflow-apiserver "${add_cmd[@]}" 2>&1 | tee "$_log" | grep -qE "Connection|success|Successfully|created"; then
        log_success "  Connexion '$conn_id' creee"
        CONNS_SUCCESS=$((CONNS_SUCCESS + 1))
    else
        log_error "  Echec pour '$conn_id'"
        cat "$_log"
        CONNS_FAILED=$((CONNS_FAILED + 1))
    fi
    rm -f "$_log"
done

echo ""
log_info "Connexions: $CONNS_SUCCESS reussies, $CONNS_FAILED echecs"

###############################################################################
# 5. Vérification finale
###############################################################################

echo ""
log_info "Vérification finale..."

echo ""
log_info "=== Variables Airflow ==="
$DOCKER_CMD exec -T airflow-apiserver airflow variables list

echo ""
log_info "=== Connexions Airflow ==="
$DOCKER_CMD exec -T airflow-apiserver airflow connections list

###############################################################################
# Résumé
###############################################################################

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                         RÉSUMÉ                                ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_info "Variables configurées  : $VARS_SUCCESS/$((VARS_SUCCESS + VARS_FAILED))"
log_info "Connexions configurées : $CONNS_SUCCESS/$((CONNS_SUCCESS + CONNS_FAILED))"

if [ $VARS_FAILED -gt 0 ] || [ $CONNS_FAILED -gt 0 ]; then
    echo ""
    log_warning "Certaines configurations ont échoué"
    log_info "Pour diagnostiquer:"
    echo "  - Vérifiez les logs: ./manage.sh logs airflow-apiserver"
    echo "  - Lancez le diagnostic: ./scripts/manage/diagnose.sh"
    echo "  - Vérifiez les fichiers JSON"
else
    echo ""
    log_success "Configuration terminée avec succès!"
    log_info "Accédez à http://localhost:8080"
fi

echo ""
