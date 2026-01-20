#!/bin/bash

###############################################################################
# Script de diagnostic Airflow
# Vérifie l'état du système et diagnostique les problèmes
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
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_section() { echo -e "\n${CYAN}=== $1 ===${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Détecte docker-compose
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                    Diagnostic Airflow AMUE                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

###############################################################################
# 1. Vérification des containers Docker
###############################################################################

log_section "1. État des Containers Docker"

if ! $DOCKER_CMD ps >/dev/null 2>&1; then
    log_error "Docker Compose n'est pas accessible ou les services ne sont pas démarrés"
    log_info "Essayez: $DOCKER_CMD up -d"
    exit 1
fi

# Liste les containers
$DOCKER_CMD ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Vérifie chaque service
log_section "1.1. Vérification des Services Critiques"

check_service() {
    local service=$1
    if $DOCKER_CMD ps | grep -q "$service"; then
        if $DOCKER_CMD ps | grep "$service" | grep -q "Up"; then
            log_success "$service est en cours d'exécution"
            return 0
        else
            log_error "$service n'est pas en état 'Up'"
            return 1
        fi
    else
        log_error "$service n'existe pas"
        return 1
    fi
}

check_service "airflow-apiserver"
check_service "airflow-scheduler"
check_service "airflow-dag-processor"
check_service "postgres"
check_service "postgres-data"

###############################################################################
# 2. Vérification de l'API Airflow
###############################################################################

log_section "2. Vérification de l'API Airflow"

if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/version | grep -q "200"; then
    log_success "API Airflow est accessible (HTTP 200)"

    # Récupère la version
    VERSION=$(curl -s http://localhost:8080/api/v2/version 2>/dev/null | jq -r '.version' 2>/dev/null || echo "N/A")
    log_info "Version Airflow: $VERSION"
else
    log_error "API Airflow n'est pas accessible sur http://localhost:8080"
    log_info "Vérifiez les logs: $DOCKER_CMD logs airflow-apiserver"
fi

###############################################################################
# 3. Vérification de la Base de Données
###############################################################################

log_section "3. Vérification des Bases de Données"

# PostgreSQL Airflow
log_info "PostgreSQL Airflow (metadata):"
if $DOCKER_CMD exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
    log_success "Base Airflow accessible"

    # Compte les DAGs
    DAG_COUNT=$($DOCKER_CMD exec -T postgres psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM dag" 2>/dev/null | tr -d ' ' || echo "0")
    log_info "Nombre de DAGs dans la BDD: $DAG_COUNT"
else
    log_error "Base Airflow non accessible"
fi

# PostgreSQL Data
log_info "PostgreSQL Data (business_data):"
if $DOCKER_CMD exec -T postgres-data pg_isready -U datauser >/dev/null 2>&1; then
    log_success "Base Data accessible"

    # Vérifie le schéma splus
    SCHEMA_EXISTS=$($DOCKER_CMD exec -T postgres-data psql -U datauser -d business_data -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'splus')" 2>/dev/null | tr -d ' ' || echo "f")
    if [ "$SCHEMA_EXISTS" = "t" ]; then
        log_success "Schéma 'splus' existe"

        # Compte les tables
        TABLE_COUNT=$($DOCKER_CMD exec -T postgres-data psql -U datauser -d business_data -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'splus'" 2>/dev/null | tr -d ' ' || echo "0")
        log_info "Nombre de tables dans 'splus': $TABLE_COUNT"
    else
        log_warning "Schéma 'splus' n'existe pas"
    fi
else
    log_error "Base Data non accessible"
fi

###############################################################################
# 4. Vérification des Fichiers de Configuration
###############################################################################

log_section "4. Fichiers de Configuration"

check_file() {
    local file=$1
    local label=$2

    if [ -f "$file" ]; then
        log_success "$label existe"

        # Vérifie si c'est un JSON valide
        if [[ "$file" == *.json ]]; then
            if jq empty "$file" 2>/dev/null; then
                log_success "$label est un JSON valide"
            else
                log_error "$label n'est pas un JSON valide!"
            fi
        fi
    else
        log_error "$label est manquant: $file"
    fi
}

check_file "$PROJECT_DIR/config/airflow_variables.json" "Variables Airflow"
check_file "$PROJECT_DIR/config/airflow_connections.json" "Connexions Airflow"
check_file "$PROJECT_DIR/.env" "Fichier .env"
check_file "$PROJECT_DIR/docker-compose.yml" "Docker Compose"

###############################################################################
# 5. Vérification des Variables Airflow
###############################################################################

log_section "5. Variables Airflow"

if $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    VAR_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow variables list 2>/dev/null | tail -n +3 | wc -l || echo "0")
    log_info "Nombre de variables configurées: $VAR_COUNT"

    # Vérifie les variables critiques
    CRITICAL_VARS=("environment" "oauth_api_connection_id" "amue_tables_to_import")

    for var in "${CRITICAL_VARS[@]}"; do
        if $DOCKER_CMD exec -T airflow-apiserver airflow variables get "$var" >/dev/null 2>&1; then
            log_success "Variable '$var' existe"
        else
            log_error "Variable '$var' est manquante"
        fi
    done

    # Affiche toutes les variables
    echo ""
    log_info "Liste complète des variables:"
    $DOCKER_CMD exec -T airflow-apiserver airflow variables list 2>/dev/null || log_error "Impossible de lister les variables"
else
    log_error "Service airflow-apiserver non démarré"
fi

###############################################################################
# 6. Vérification des Connexions Airflow
###############################################################################

log_section "6. Connexions Airflow"

if $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    CONN_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow connections list 2>/dev/null | tail -n +3 | wc -l || echo "0")
    log_info "Nombre de connexions configurées: $CONN_COUNT"

    # Vérifie les connexions critiques
    CRITICAL_CONNS=("oauth_api" "postgres_data")

    for conn in "${CRITICAL_CONNS[@]}"; do
        if $DOCKER_CMD exec -T airflow-apiserver airflow connections get "$conn" >/dev/null 2>&1; then
            log_success "Connexion '$conn' existe"

            # Test la connexion si possible
            echo -n "  Test de connexion... "
            if $DOCKER_CMD exec -T airflow-apiserver airflow connections test "$conn" 2>&1 | grep -q "success\|Connection successfully tested"; then
                echo -e "${GREEN}✓ OK${NC}"
            else
                echo -e "${YELLOW}⚠ Échec du test${NC}"
            fi
        else
            log_error "Connexion '$conn' est manquante"
        fi
    done

    # Affiche toutes les connexions
    echo ""
    log_info "Liste complète des connexions:"
    $DOCKER_CMD exec -T airflow-apiserver airflow connections list 2>/dev/null || log_error "Impossible de lister les connexions"
else
    log_error "Service airflow-apiserver non démarré"
fi

###############################################################################
# 7. Vérification des DAGs
###############################################################################

log_section "7. DAGs Airflow"

if $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    # Liste les DAGs
    log_info "DAGs détectés:"
    $DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null || log_error "Impossible de lister les DAGs"

    echo ""

    # Vérifie les erreurs de parsing
    log_info "Erreurs de parsing des DAGs:"
    PARSE_ERRORS=$($DOCKER_CMD exec -T airflow-apiserver airflow dags list-import-errors 2>/dev/null | tail -n +3 | wc -l || echo "0")

    if [ "$PARSE_ERRORS" -eq 0 ]; then
        log_success "Aucune erreur de parsing"
    else
        log_error "$PARSE_ERRORS erreur(s) de parsing détectée(s)"
        $DOCKER_CMD exec -T airflow-apiserver airflow dags list-import-errors 2>/dev/null
    fi

    # Vérifie le DAG principal
    echo ""
    if $DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null | grep -q "amue_multi_table_import"; then
        log_success "DAG 'amue_multi_table_import' détecté"
    else
        log_warning "DAG 'amue_multi_table_import' non trouvé"
    fi
else
    log_error "Service airflow-apiserver non démarré"
fi

###############################################################################
# 8. Vérification des Logs
###############################################################################

log_section "8. Logs Récents (Erreurs)"

log_info "Scheduler (dernières erreurs):"
$DOCKER_CMD logs airflow-scheduler --tail=20 2>&1 | grep -i "error\|exception\|failed" || echo "Aucune erreur récente"

echo ""
log_info "API Server (dernières erreurs):"
$DOCKER_CMD logs airflow-apiserver --tail=20 2>&1 | grep -i "error\|exception\|failed" || echo "Aucune erreur récente"

###############################################################################
# 9. Recommandations
###############################################################################

log_section "9. Recommandations"

# Variables manquantes
VAR_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow variables list 2>/dev/null | tail -n +3 | wc -l || echo "0")
if [ "$VAR_COUNT" -lt 5 ]; then
    log_warning "Peu de variables configurées ($VAR_COUNT)"
    echo "  → Exécutez: ./manage.sh config"
fi

# Connexions manquantes
CONN_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow connections list 2>/dev/null | tail -n +3 | wc -l || echo "0")
if [ "$CONN_COUNT" -lt 2 ]; then
    log_warning "Peu de connexions configurées ($CONN_COUNT)"
    echo "  → Vérifiez: config/airflow_connections.json"
    echo "  → Exécutez: ./manage.sh config"
fi

# DAGs
if ! $DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null | grep -q "amue"; then
    log_warning "Aucun DAG AMUE détecté"
    echo "  → Vérifiez le dossier: dags/"
    echo "  → Vérifiez les logs: ./manage.sh logs dag-processor"
fi

###############################################################################
# Résumé
###############################################################################

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                      RÉSUMÉ DU DIAGNOSTIC                     ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_info "Services Docker       : $(if $DOCKER_CMD ps | grep -q 'Up'; then echo -e "${GREEN}OK${NC}"; else echo -e "${RED}KO${NC}"; fi)"
log_info "API Airflow          : $(if curl -s http://localhost:8080/api/v2/version >/dev/null 2>&1; then echo -e "${GREEN}OK${NC}"; else echo -e "${RED}KO${NC}"; fi)"
log_info "Base Airflow         : $(if $DOCKER_CMD exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then echo -e "${GREEN}OK${NC}"; else echo -e "${RED}KO${NC}"; fi)"
log_info "Base Data            : $(if $DOCKER_CMD exec -T postgres-data pg_isready -U datauser >/dev/null 2>&1; then echo -e "${GREEN}OK${NC}"; else echo -e "${RED}KO${NC}"; fi)"
log_info "Variables ($VAR_COUNT)      : $(if [ "$VAR_COUNT" -ge 10 ]; then echo -e "${GREEN}OK${NC}"; else echo -e "${YELLOW}À vérifier${NC}"; fi)"
log_info "Connexions ($CONN_COUNT)     : $(if [ "$CONN_COUNT" -ge 2 ]; then echo -e "${GREEN}OK${NC}"; else echo -e "${YELLOW}À vérifier${NC}"; fi)"

echo ""
log_info "Pour plus de détails, consultez les logs:"
echo "  - Scheduler: ./manage.sh logs airflow-scheduler"
echo "  - API Server: ./manage.sh logs airflow-apiserver"
echo "  - DAG Processor: ./manage.sh logs airflow-dag-processor"

echo ""
log_info "Accès Web: http://localhost:8080"
log_info "Username: airflow"
log_info "Password: airflow"

echo ""