#!/bin/bash

###############################################################################
# Script pour s'assurer que MailHog est démarré
# S'exécute automatiquement au démarrage
###############################################################################

set -e

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

echo "[INFO] Vérification MailHog..."

# Vérifie si MailHog est déjà en cours
if $DOCKER_CMD ps 2>/dev/null | grep -q "mailhog.*Up"; then
    echo "[OK] MailHog est déjà en cours d'exécution"
    exit 0
fi

# Démarre MailHog
echo "[INFO] Démarrage de MailHog..."
$DOCKER_CMD up -d mailhog

# Attend que MailHog soit prêt
for i in {1..10}; do
    if $DOCKER_CMD ps 2>/dev/null | grep -q "mailhog.*Up"; then
        echo "[OK] MailHog démarré avec succès"
        echo "[INFO] Interface disponible sur: http://localhost:8025"
        exit 0
    fi
    sleep 1
done

echo "[WARNING] MailHog pourrait ne pas être complètement démarré"
exit 0