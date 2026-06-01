#!/bin/bash
#
# Script de démonstration des cas d'échec
# Usage: ./scripts/demo_failures.sh [demo_number]
#

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo -e "${GREEN}➜${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

wait_for_user() {
    echo ""
    echo -n "Appuyez sur Entrée pour continuer..." >&2
    read -r </dev/tty
    echo ""
}

# Menu principal
show_menu() {
    print_header "Démonstrations des Cas d'Échec AMUE"

    echo "Sélectionnez une démonstration :"
    echo ""
    echo "  1) Table absente de l'API"
    echo "  2) API indisponible (timeout simulé)"
    echo "  3) Afficher les logs en temps réel"
    echo "  4) Ouvrir MailHog (emails)"
    echo "  5) Ouvrir Airflow UI"
    echo "  6) Restaurer configuration normale"
    echo ""
    echo "  0) Quitter"
    echo ""
}

# Démo 1: Table absente
demo_missing_table() {
    print_header "Démo 1: Table Absente de l'API"

    print_step "Cette démo ajoute une table fictive 'TABLE_INEXISTANTE' à la configuration"
    print_warning "Le DAG va échouer car cette table n'existe pas dans l'API AMUE"

    wait_for_user

    print_step "Sauvegarde de la configuration actuelle..."

    # Récupérer la config actuelle via docker
    _backup_file=$(mktemp --suffix=.json)
    docker exec airflow-apiserver airflow variables get amue_tables_to_import > "$_backup_file" 2>/dev/null || true

    print_step "Ajout d'une table fictive à la configuration..."

    # Créer une config avec une table inexistante
    NEW_CONFIG='[{"name":"CSKS","enable":true,"primary_key":"","delta":"","last_import":"","finger_print":""},{"name":"TABLE_INEXISTANTE","enable":true,"primary_key":"","delta":"","last_import":"","finger_print":""}]'

    docker exec airflow-apiserver airflow variables set amue_tables_to_import "$NEW_CONFIG"

    print_info "Configuration modifiée. Nouvelle config :"
    echo "$NEW_CONFIG" | python3 -m json.tool 2>/dev/null || echo "$NEW_CONFIG"

    echo ""
    print_step "Déclenchement du DAG..."
    docker exec airflow-apiserver airflow dags trigger amue_multi_table_import

    echo ""
    print_info "Le DAG a été déclenché."
    print_info "Ouvrez Airflow UI (http://localhost:8080) pour voir l'échec"
    print_info "Ouvrez MailHog (http://localhost:8025) pour voir l'email d'erreur"

    echo ""
    print_warning "Pour restaurer la configuration normale, lancez l'option 6"
}

# Démo 2: API indisponible
demo_api_timeout() {
    print_header "Démo 2: API Indisponible (Simulation)"

    print_step "Cette démo modifie l'endpoint API pour simuler une indisponibilité"
    print_warning "Le système va attendre et réessayer plusieurs fois"

    wait_for_user

    print_step "Sauvegarde de l'endpoint actuel..."
    _endpoint_backup=$(mktemp --suffix=.txt)
    CURRENT_ENDPOINT=$(docker exec airflow-apiserver airflow variables get api_endpoint_table 2>/dev/null || echo "")
    echo "$CURRENT_ENDPOINT" > "$_endpoint_backup"

    print_step "Modification de l'endpoint vers une URL invalide..."
    docker exec airflow-apiserver airflow variables set api_endpoint_table "invalid/endpoint/that/does/not/exist"

    print_info "Endpoint modifié vers URL invalide"

    echo ""
    print_step "Modification du timeout pour accélérer la démo..."
    docker exec airflow-apiserver airflow variables set amue_max_wait_hours "0.05"
    docker exec airflow-apiserver airflow variables set amue_polling_interval_minutes "1"

    echo ""
    print_step "Déclenchement du DAG..."
    docker exec airflow-apiserver airflow dags trigger amue_multi_table_import

    echo ""
    print_info "Le DAG a été déclenché."
    print_info "Observez les logs pour voir les tentatives de retry"
    print_info "Commande: ./manage.sh logs airflow-scheduler -f"

    echo ""
    print_warning "Pour restaurer la configuration normale, lancez l'option 6"
}

# Afficher les logs
show_logs() {
    print_header "Logs Airflow en temps réel"
    print_info "Appuyez sur Ctrl+C pour arrêter"
    echo ""

    docker logs -f airflow-scheduler 2>&1 | grep -E "(IMPORT|FILTER|POLLING|ERROR|WARN|INFO)" || true
}

# Ouvrir MailHog
open_mailhog() {
    print_header "Ouverture MailHog"

    URL="http://localhost:8025"
    print_info "MailHog est accessible sur: $URL"

    # Tenter d'ouvrir le navigateur
    if command -v xdg-open &> /dev/null; then
        xdg-open "$URL" 2>/dev/null &
    elif command -v open &> /dev/null; then
        open "$URL" 2>/dev/null &
    elif command -v start &> /dev/null; then
        start "$URL" 2>/dev/null &
    else
        print_warning "Impossible d'ouvrir automatiquement. Ouvrez manuellement: $URL"
    fi
}

# Ouvrir Airflow UI
open_airflow() {
    print_header "Ouverture Airflow UI"

    URL="http://localhost:8080"
    print_info "Airflow est accessible sur: $URL"
    print_info "Identifiants: admin / admin"

    # Tenter d'ouvrir le navigateur
    if command -v xdg-open &> /dev/null; then
        xdg-open "$URL" 2>/dev/null &
    elif command -v open &> /dev/null; then
        open "$URL" 2>/dev/null &
    elif command -v start &> /dev/null; then
        start "$URL" 2>/dev/null &
    else
        print_warning "Impossible d'ouvrir automatiquement. Ouvrez manuellement: $URL"
    fi
}

# Restaurer configuration
restore_config() {
    print_header "Restauration de la Configuration"

    print_step "Restauration de la configuration des tables..."

    # Config par défaut
    DEFAULT_CONFIG='[{"name":"CSKS","enable":true,"primary_key":"","delta":"","last_import":"","finger_print":""},{"name":"COVP","enable":true,"primary_key":"","delta":"","last_import":"","finger_print":""},{"name":"CEPC","enable":true,"primary_key":"","delta":"","last_import":"","finger_print":""},{"name":"EKET","enable":true,"primary_key":"","delta":"bedat","last_import":"","finger_print":""}]'

    docker exec airflow-apiserver airflow variables set amue_tables_to_import "$DEFAULT_CONFIG" 2>/dev/null || true

    print_step "Restauration de l'endpoint API..."
    DEFAULT_ENDPOINT='finances/cdv/v1/preprod/${univ}/table'
    docker exec airflow-apiserver airflow variables set api_endpoint_table "$DEFAULT_ENDPOINT" 2>/dev/null || true

    print_step "Restauration des timeouts..."
    docker exec airflow-apiserver airflow variables set amue_max_wait_hours "6" 2>/dev/null || true
    docker exec airflow-apiserver airflow variables set amue_polling_interval_minutes "10" 2>/dev/null || true

    echo ""
    print_info "Configuration restaurée avec succès!"
}

# Main
main() {
    # Vérifier que Docker est accessible
    if ! docker ps &> /dev/null; then
        print_error "Docker n'est pas accessible. Lancez d'abord: ./manage.sh start"
        exit 1
    fi

    # Si argument passé, exécuter directement
    if [ -n "$1" ]; then
        case $1 in
            1) demo_missing_table ;;
            2) demo_api_timeout ;;
            3) show_logs ;;
            4) open_mailhog ;;
            5) open_airflow ;;
            6) restore_config ;;
            *) echo "Option invalide: $1" ;;
        esac
        exit 0
    fi

    # Menu interactif
    while true; do
        show_menu
        echo -n "Votre choix : " >&2
        read -r choice </dev/tty

        case $choice in
            1) demo_missing_table ;;
            2) demo_api_timeout ;;
            3) show_logs ;;
            4) open_mailhog ;;
            5) open_airflow ;;
            6) restore_config ;;
            0)
                print_info "Au revoir!"
                exit 0
                ;;
            *)
                print_error "Option invalide"
                ;;
        esac

        wait_for_user
    done
}

main "$@"
