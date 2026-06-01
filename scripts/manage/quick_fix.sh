#!/bin/bash

###############################################################################
# Script de test rapide de la configuration
# Vérifie que tout est correctement configuré
###############################################################################

set -e
set -o pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_SCRIPT_DIR/../lib/colors.sh"
# Surcharge : format [✓]/[✗] spécifique à ce script
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_error()   { echo -e "${RED}[✗]${NC} $1"; }

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

PASSED=0
FAILED=0

test_check() {
    local name=$1
    local cmd=$2

    if bash -c "$cmd" >/dev/null 2>&1; then
        log_success "$name"
        ((PASSED++))
        return 0
    else
        log_error "$name"
        ((FAILED++))
        return 1
    fi
}

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║               Test Rapide Configuration Airflow               ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_info "Exécution des tests..."
echo ""

# Services
log_info "=== Services ==="
test_check "Service airflow-apiserver en cours" "$DOCKER_CMD ps | grep -q 'airflow-apiserver.*Up'"
test_check "Service airflow-scheduler en cours" "$DOCKER_CMD ps | grep -q 'airflow-scheduler.*Up'"
test_check "Service postgres en cours" "$DOCKER_CMD ps | grep -q 'postgres.*Up'"
test_check "Service postgres-data en cours" "$DOCKER_CMD ps | grep -q 'postgres-data.*Up'"

echo ""
log_info "=== API ==="
test_check "API Airflow accessible" "curl -s http://localhost:8080/api/v2/version | grep -q version"

echo ""
log_info "=== Variables Critiques ==="
test_check "Variable 'universite' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow variables get universite 2>/dev/null"
test_check "Variable 'api_endpoint_admin' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow variables get api_endpoint_admin 2>/dev/null"
test_check "Variable 'api_endpoint_table' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow variables get api_endpoint_table 2>/dev/null"
test_check "Variable 'amue_import_batch_size' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow variables get amue_import_batch_size 2>/dev/null"

echo ""
log_info "=== Connexions ==="
test_check "Connexion 'oauth_api' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow connections get oauth_api 2>/dev/null"
test_check "Connexion 'postgres_data' existe" "$DOCKER_CMD exec -T airflow-apiserver airflow connections get postgres_data 2>/dev/null"

echo ""
log_info "=== Base de Données ==="
test_check "Base Airflow accessible" "$DOCKER_CMD exec -T postgres pg_isready -q"
test_check "Base Data accessible" "$DOCKER_CMD exec -T postgres-data pg_isready -q"

echo ""
log_info "=== DAGs ==="
test_check "DAG AMUE détecté" "$DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null | grep -q amue"

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                          RÉSULTATS                            ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_info "Tests réussis : ${GREEN}$PASSED${NC}"
log_info "Tests échoués : ${RED}$FAILED${NC}"

echo ""

if [ $FAILED -eq 0 ]; then
    cat << EOF
${GREEN}✓ Tous les tests sont passés !${NC}

Votre installation est correcte. Vous pouvez :

1. Accéder à l'interface : http://localhost:8080
   - Username: airflow
   - Password: airflow

2. Déclencher un import :
   ./manage.sh trigger amue_multi_table_import

3. Voir les logs :
   ./manage.sh logs scheduler

EOF
    exit 0
else
    cat << EOF
${RED}✗ Certains tests ont échoué${NC}

Recommandations :

1. Vérifier le diagnostic complet :
   ./manage.sh diagnose

2. Corriger la configuration :
   ./manage.sh fix

3. Redémarrer si nécessaire :
   ./manage.sh restart
   sleep 60
   ./manage.sh fix

4. Consulter la documentation :
   cat TROUBLESHOOTING_CONFIG.md

EOF
    exit 1
fi