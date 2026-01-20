#!/bin/bash

###############################################################################
# Script de correction de la configuration Airflow
# Attend que l'API soit prête puis configure les variables et connexions
###############################################################################

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
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
if ! $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    log_error "Le service airflow-apiserver n'est pas démarré"
    log_info "Démarrez avec: ./manage.sh start"
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

# Attente supplémentaire pour s'assurer que les commandes CLI sont prêtes
log_info "Attente supplémentaire (5s) pour la CLI..."
sleep 5

###############################################################################
# 3. Configuration des Variables
###############################################################################

log_info "Étape 3/4: Configuration des Variables"

VARS_SUCCESS=0
VARS_FAILED=0

# Variables simples
for key in $(jq -r 'to_entries | map(select(.value | type != "array" and type != "object")) | .[].key' "$VARIABLES_FILE"); do
    log_info "Variable trouvé: $key"
done
for key in $(jq -r 'to_entries | map(select(.value | type != "array" and type != "object")) | .[].key' "$VARIABLES_FILE"); do
    value=$(jq -r ".[\"$key\"]" "$VARIABLES_FILE")

    log_info "Configuration de la variable: $key"
    log_info "  Valeur: $value"
    # Tentative de création
    if $DOCKER_CMD exec -T airflow-apiserver airflow variables set "$key" "$value" | tee /tmp/airflow_var_output.log 2>&1 | grep -qE "Variable|success|created|updated|set"; then
        log_success "  ✓ Variable '$key' créée"
        VARS_SUCCESS=$((VARS_SUCCESS + 1))
    else
        log_error "  ✗ Échec pour '$key'"
        cat /tmp/airflow_var_output.log
        VARS_FAILED=$((VARS_FAILED + 1))
    fi
done

# Variables complexes (JSON)
for key in $(jq -r 'to_entries | map(select(.value | type == "array" or type == "object")) | .[].key' "$VARIABLES_FILE"); do
    value=$(jq -c ".[\"$key\"]" "$VARIABLES_FILE")

    log_info "Configuration de la variable JSON: $key"

    # Crée un fichier temporaire pour éviter les problèmes d'échappement
    echo "$value" > /tmp/airflow_var_value.json

    # Tentative de création
    if $DOCKER_CMD exec -T airflow-apiserver bash -c "airflow variables set '$key' '$(cat /tmp/airflow_var_value.json)'" 2>&1 | tee /tmp/airflow_var_output.log | grep -qE "Variable|success|created|updated|set"; then
        log_success "  ✓ Variable JSON '$key' créée"
        VARS_SUCCESS=$((VARS_SUCCESS + 1))
    else
        log_error "  ✗ Échec pour '$key'"
        cat /tmp/airflow_var_output.log
        VARS_FAILED=$((VARS_FAILED + 1))
    fi

    rm -f /tmp/airflow_var_value.json
done

rm -f /tmp/airflow_var_output.log

echo ""
log_info "Variables: $VARS_SUCCESS réussies, $VARS_FAILED échecs"

###############################################################################
# 4. Configuration des Connexions
###############################################################################

log_info "Étape 4/4: Configuration des Connexions"

CONNS_SUCCESS=0
CONNS_FAILED=0

for conn_id in $(jq -r 'keys[]' "$CONNECTIONS_FILE"); do
    log_info "Configuration de la connexion: $conn_id"

    conn_type=$(jq -r ".[\"$conn_id\"].conn_type" "$CONNECTIONS_FILE")
    host=$(jq -r ".[\"$conn_id\"].host // empty" "$CONNECTIONS_FILE")
    login=$(jq -r ".[\"$conn_id\"].login // empty" "$CONNECTIONS_FILE")
    password=$(jq -r ".[\"$conn_id\"].password // empty" "$CONNECTIONS_FILE")
    port=$(jq -r ".[\"$conn_id\"].port // empty" "$CONNECTIONS_FILE")
    schema=$(jq -r ".[\"$conn_id\"].schema // empty" "$CONNECTIONS_FILE")
    extra=$(jq -c ".[\"$conn_id\"].extra // {}" "$CONNECTIONS_FILE")

    # Supprime la connexion existante
    $DOCKER_CMD exec -T airflow-apiserver airflow connections delete "$conn_id" >/dev/null 2>&1 || true

    # Construction de la commande
    cmd="airflow connections add \"$conn_id\" --conn-type \"$conn_type\""

    [ -n "$host" ] && cmd="$cmd --conn-host \"$host\""
    [ -n "$login" ] && cmd="$cmd --conn-login \"$login\""
    [ -n "$password" ] && cmd="$cmd --conn-password \"$password\""
    [ -n "$port" ] && cmd="$cmd --conn-port \"$port\""
    [ -n "$schema" ] && cmd="$cmd --conn-schema \"$schema\""

    # Traitement spécial pour extra (doit être en JSON valide)
    if [ "$extra" != "{}" ] && [ "$extra" != "null" ]; then
        # Échappe correctement le JSON pour bash
        extra_escaped=$(echo "$extra" | sed "s/'/'\\\\''/g")
        cmd="$cmd --conn-extra '$extra_escaped'"
    fi

    log_info "  Commande: $cmd"

    # Crée la connexion
    if $DOCKER_CMD exec -T airflow-apiserver bash -c "$cmd" 2>&1 | tee /tmp/airflow_conn_output.log | grep -qE "Connection|success|Successfully|created"; then
        log_success "  ✓ Connexion '$conn_id' créée"
        CONNS_SUCCESS=$((CONNS_SUCCESS + 1))
    else
        log_error "  ✗ Échec pour '$conn_id'"
        cat /tmp/airflow_conn_output.log
        CONNS_FAILED=$((CONNS_FAILED + 1))
    fi
done

rm -f /tmp/airflow_conn_output.log

echo ""
log_info "Connexions: $CONNS_SUCCESS réussies, $CONNS_FAILED échecs"

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