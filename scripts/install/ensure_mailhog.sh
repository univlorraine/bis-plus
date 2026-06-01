#!/bin/bash

###############################################################################
# Script pour s'assurer que MailHog est démarré
# S'exécute automatiquement au démarrage
###############################################################################

set -e
set -o pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_SCRIPT_DIR/../lib/colors.sh"

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

log_info "Vérification MailHog..."

if $DOCKER_CMD ps 2>/dev/null | grep -q "mailhog.*Up"; then
    log_success "MailHog est déjà en cours d'exécution"
    exit 0
fi

log_info "Démarrage de MailHog..."
$DOCKER_CMD up -d mailhog

_retries=10
while [[ $_retries -gt 0 ]]; do
    if $DOCKER_CMD ps 2>/dev/null | grep -q "mailhog.*Up"; then
        log_success "MailHog démarré avec succès"
        log_info "Interface disponible sur: http://localhost:8025"
        exit 0
    fi
    ((_retries-=1))
    sleep 1
done

log_warning "MailHog pourrait ne pas être complètement démarré"
exit 0