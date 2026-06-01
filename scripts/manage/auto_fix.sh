#!/bin/bash

###############################################################################
# Script de correction automatique complète
# Détecte les problèmes et les corrige automatiquement
###############################################################################

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

source "$SCRIPT_DIR/../lib/colors.sh"
log_step() { echo -e "\n${CYAN}▶ $1${NC}\n"; }

# Détecte docker-compose
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                Correction Automatique Complète                ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

Ce script va :
1. Détecter les problèmes
2. Les corriger automatiquement
3. Vérifier que tout fonctionne

EOF

echo -n "Continuer ? (o/N) : " >&2
read -r _CONFIRM </dev/tty
if [[ ! "$_CONFIRM" =~ ^[oOyY]$ ]]; then
    log_info "Opération annulée"
    exit 0
fi

###############################################################################
# Détection des problèmes
###############################################################################

log_step "Étape 1/5: Détection des problèmes"

ISSUES=()

# Services
if ! $DOCKER_CMD ps | grep -q "airflow-apiserver.*Up"; then
    ISSUES+=("services_down")
    log_warning "Services Airflow non démarrés"
else
    log_success "Services en cours d'exécution"
fi

# API
if ! curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/version 2>/dev/null | grep -q "200"; then
    ISSUES+=("api_not_ready")
    log_warning "API Airflow non accessible"
else
    log_success "API Airflow accessible"
fi

# Variables
if $DOCKER_CMD ps | grep -q "airflow-apiserver.*Up"; then
    VAR_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow variables list --output json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,(list,dict)) else 0)" 2>/dev/null || echo "0")
    if [ "$VAR_COUNT" -lt 5 ]; then
        ISSUES+=("missing_variables")
        log_warning "Variables manquantes (seulement $VAR_COUNT configurées)"
    else
        log_success "Variables OK ($VAR_COUNT configurées)"
    fi
fi

# Connexions
if $DOCKER_CMD ps | grep -q "airflow-apiserver.*Up"; then
    CONN_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow connections list --output json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,(list,dict)) else 0)" 2>/dev/null || echo "0")
    if [ "$CONN_COUNT" -lt 2 ]; then
        ISSUES+=("missing_connections")
        log_warning "Connexions manquantes (seulement $CONN_COUNT configurées)"
    else
        log_success "Connexions OK ($CONN_COUNT configurées)"
    fi
fi

echo ""
log_info "Problèmes détectés: ${#ISSUES[@]}"

if [ ${#ISSUES[@]} -eq 0 ]; then
    log_success "Aucun problème détecté ! Tout fonctionne correctement."
    exit 0
fi

###############################################################################
# Correction des problèmes
###############################################################################

log_step "Étape 2/5: Correction des problèmes"

# Services down
if [[ " ${ISSUES[@]} " =~ " services_down " ]]; then
    log_info "Redémarrage des services..."
    $DOCKER_CMD restart
    _r=10
    while [[ $_r -gt 0 ]]; do
        if $DOCKER_CMD ps | grep -q "airflow-apiserver.*Up"; then
            log_success "Services redémarrés"
            break
        fi
        ((_r-=1))
        sleep 3
    done
    if [[ $_r -eq 0 ]]; then
        log_error "Échec du redémarrage. Essayez manuellement: ./manage.sh restart"
        exit 1
    fi
fi

# API non prête
if [[ " ${ISSUES[@]} " =~ " api_not_ready " ]]; then
    log_info "Attente de l'API Airflow (jusqu'à 2 minutes)..."

    MAX_WAIT=60
    WAITED=0

    while [ $WAITED -lt $MAX_WAIT ]; do
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/version 2>/dev/null | grep -q "200"; then
            log_success "API Airflow prête"
            break
        fi

        sleep 2
        WAITED=$((WAITED + 1))

        if [ $WAITED -ge $MAX_WAIT ]; then
            log_error "Timeout: API non disponible après 2 minutes"
            log_info "Vérifiez les logs: ./manage.sh logs airflow-apiserver"
            exit 1
        fi
    done

fi

# Variables manquantes
if [[ " ${ISSUES[@]} " =~ " missing_variables " ]] || [[ " ${ISSUES[@]} " =~ " missing_connections " ]]; then
    log_info "Configuration des variables et connexions..."

    chmod +x "$SCRIPT_DIR/fix_config.sh"

    if "$SCRIPT_DIR/fix_config.sh"; then
        log_success "Configuration appliquée"
    else
        log_error "Échec de la configuration"
        log_info "Essayez manuellement: ./manage.sh fix"
        exit 1
    fi
fi

###############################################################################
# Vérification post-correction
###############################################################################

log_step "Étape 3/5: Vérification"

# Test rapide
log_info "Exécution du test rapide..."

chmod +x "$SCRIPT_DIR/quick_fix.sh"

if "$SCRIPT_DIR/quick_fix.sh"; then
    log_success "Tous les tests sont passés"
else
    log_warning "Certains tests ont échoué"
    log_info "Lancez un diagnostic: ./manage.sh diagnose"
fi

###############################################################################
# Vérification des DAGs
###############################################################################

log_step "Étape 4/5: Vérification des DAGs"

if $DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null | grep -q "amue"; then
    log_success "DAG AMUE détecté"

    # Liste les DAGs AMUE
    echo ""
    log_info "DAGs AMUE disponibles:"
    $DOCKER_CMD exec -T airflow-apiserver airflow dags list 2>/dev/null | grep amue | while read line; do
        echo "  - $line"
    done
else
    log_warning "DAG AMUE non détecté"
    log_info "Vérifiez le dossier dags/ et les logs du dag-processor"
fi

###############################################################################
# Résumé et prochaines étapes
###############################################################################

log_step "Étape 5/5: Résumé"

# Récupère les stats finales
VAR_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow variables list --output json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,(list,dict)) else 0)" 2>/dev/null || echo "0")
CONN_COUNT=$($DOCKER_CMD exec -T airflow-apiserver airflow connections list --output json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d) if isinstance(d,(list,dict)) else 0)" 2>/dev/null || echo "0")
API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/version 2>/dev/null || echo "000")

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                      RÉSUMÉ FINAL                             ║
╚═══════════════════════════════════════════════════════════════╝

État du système:
  - Services Docker    : $(if $DOCKER_CMD ps | grep -q 'Up'; then echo -e "${GREEN}OK${NC}"; else echo -e "${RED}KO${NC}"; fi)
  - API Airflow        : $(if [ "$API_STATUS" = "200" ]; then echo -e "${GREEN}OK (HTTP $API_STATUS)${NC}"; else echo -e "${RED}KO (HTTP $API_STATUS)${NC}"; fi)
  - Variables          : $(if [ "$VAR_COUNT" -ge 10 ]; then echo -e "${GREEN}$VAR_COUNT configurées${NC}"; else echo -e "${YELLOW}$VAR_COUNT configurées${NC}"; fi)
  - Connexions         : $(if [ "$CONN_COUNT" -ge 2 ]; then echo -e "${GREEN}$CONN_COUNT configurées${NC}"; else echo -e "${YELLOW}$CONN_COUNT configurées${NC}"; fi)

EOF

if [ "$API_STATUS" = "200" ] && [ "$VAR_COUNT" -ge 10 ] && [ "$CONN_COUNT" -ge 2 ]; then
    cat << EOF
${GREEN}✓ Correction réussie !${NC}

Prochaines étapes:

1. Accédez à l'interface Web:
   URL      : http://localhost:8080
   Username : airflow
   Password : airflow

2. Activez et déclenchez le DAG:
   ./manage.sh unpause amue_multi_table_import
   ./manage.sh trigger amue_multi_table_import

3. Suivez l'exécution:
   - Interface Web: http://localhost:8080
   - Logs: ./manage.sh logs scheduler

EOF
else
    cat << EOF
${YELLOW}⚠ Configuration partielle${NC}

Des problèmes subsistent. Recommandations:

1. Diagnostic complet:
   ./manage.sh diagnose

2. Voir les logs:
   ./manage.sh logs airflow-apiserver
   ./manage.sh logs airflow-scheduler

3. Redémarrage complet si nécessaire:
   ./manage.sh stop
   docker-compose down -v
   ./manage.sh start
   sleep 180
   ./manage.sh fix

4. Support:
   cat TROUBLESHOOTING_CONFIG.md

EOF
fi

log_info "Script terminé"