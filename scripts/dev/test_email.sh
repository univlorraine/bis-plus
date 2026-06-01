#!/bin/bash

###############################################################################
# Script de test de configuration email
# Teste l'envoi d'email via Airflow
###############################################################################

set -e
set -o pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_SCRIPT_DIR/../lib/colors.sh"

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║              Test de Configuration Email                      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

###############################################################################
# Vérifications
###############################################################################

log_info "Étape 1/4: Vérifications"

# Vérifie que les services sont démarrés
if ! $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    log_error "Service airflow-apiserver non démarré"
    exit 1
fi

if ! $DOCKER_CMD ps | grep -q "mailhog"; then
    log_warning "Service mailhog non détecté"
    log_info "Redémarrage pour inclure MailHog..."
    $DOCKER_CMD up -d mailhog
    sleep 5
fi

log_success "Services OK"

###############################################################################
# Configuration email
###############################################################################

log_info "Étape 2/4: Vérification configuration SMTP"

# Affiche la configuration actuelle
log_info "Configuration SMTP actuelle:"
$DOCKER_CMD exec -T airflow-apiserver bash -c "airflow config get-value smtp smtp_host" || echo "  smtp_host: non configuré"
$DOCKER_CMD exec -T airflow-apiserver bash -c "airflow config get-value smtp smtp_port" || echo "  smtp_port: non configuré"
$DOCKER_CMD exec -T airflow-apiserver bash -c "airflow config get-value smtp smtp_mail_from" || echo "  smtp_mail_from: non configuré"

echo ""

###############################################################################
# Test via Python
###############################################################################

log_info "Étape 3/4: Test d'envoi via Python"

# Crée un script Python de test dans un fichier temporaire
_py_tmp=$(mktemp --suffix=.py)
cat > "$_py_tmp" << 'PYEOF'
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    # Configuration SMTP
    smtp_host = 'mailhog'
    smtp_port = 1025
    from_email = 'airflow@amue-project.local'
    to_emails = ['test@example.com', 'admin@example.com']

    # Création du message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Test Email Airflow AMUE"
    msg['From'] = from_email
    msg['To'] = ', '.join(to_emails)

    # Contenu HTML
    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .header { background-color: #4CAF50; color: white; padding: 20px; }
            .content { padding: 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>✓ Test Email Réussi !</h1>
        </div>
        <div class="content">
            <p>Cet email de test a été envoyé avec succès depuis Airflow.</p>
            <p><strong>Configuration SMTP :</strong> Fonctionnelle</p>
            <p><strong>Serveur :</strong> """ + smtp_host + ":" + str(smtp_port) + """</p>
            <p><strong>Date :</strong> """ + str(__import__('datetime').datetime.now()) + """</p>
            <hr>
            <p><small>Ceci est un email de test automatique.</small></p>
        </div>
    </body>
    </html>
    """

    # Attacher le HTML
    part = MIMEText(html, 'html')
    msg.attach(part)

    print("[INFO] Envoi email de test...")
    print(f"[INFO] Destinataires: {to_emails}")
    print(f"[INFO] Serveur SMTP: {smtp_host}:{smtp_port}")

    # Connexion et envoi
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.sendmail(from_email, to_emails, msg.as_string())
    server.quit()

    print("[SUCCESS] Email envoyé avec succès!")
    sys.exit(0)

except Exception as e:
    print(f"[ERROR] Échec envoi email: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYEOF

# Copie et exécute dans le container
$DOCKER_CMD exec -T airflow-apiserver bash -c "cat > /tmp/test_email.py" < "$_py_tmp"

if $DOCKER_CMD exec -T airflow-apiserver python3 /tmp/test_email.py; then
    log_success "Email de test envoyé"
else
    log_error "Échec envoi email"
    log_info "Vérifiez les logs ci-dessus"
fi

rm -f "$_py_tmp"
$DOCKER_CMD exec -T airflow-apiserver rm -f /tmp/test_email.py 2>/dev/null || true

###############################################################################
# Vérification MailHog
###############################################################################

log_info "Étape 4/4: Vérification dans MailHog"

if $DOCKER_CMD ps | grep -q "mailhog"; then
    log_success "MailHog est accessible"

    echo ""
    log_info "Interface Web MailHog:"
    echo "  URL: http://localhost:8025"
    echo ""
    log_info "Vous devriez voir l'email de test dans l'interface"

    # Tente de récupérer les emails via l'API
    if command -v curl &> /dev/null; then
        EMAIL_COUNT=$(curl -s http://localhost:8025/api/v2/messages | jq '. | length' 2>/dev/null || echo "?")
        log_info "Emails dans la boîte de réception: $EMAIL_COUNT"
    fi
else
    log_warning "MailHog n'est pas en cours d'exécution"
fi

###############################################################################
# Résumé
###############################################################################

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                         RÉSUMÉ                                ║
╚═══════════════════════════════════════════════════════════════╝

Configuration SMTP:
  - Host: mailhog
  - Port: 1025
  - Interface Web: http://localhost:8025

Prochaines étapes:

1. Ouvrez l'interface MailHog:
   http://localhost:8025

2. Vérifiez que l'email de test est visible

3. Pour tester avec votre DAG:
   ./manage.sh trigger amue_multi_table_import

4. Configuration pour production:
   - Éditez: config/airflow.cfg partie [smtp]
   - Ou définissez les variables d'environnement dans docker-compose.yml

EOF

# Ouvre automatiquement le navigateur (si possible)
if command -v xdg-open &> /dev/null; then
    log_info "Ouverture de l'interface MailHog..."
    xdg-open http://localhost:8025 2>/dev/null &
elif command -v open &> /dev/null; then
    log_info "Ouverture de l'interface MailHog..."
    open http://localhost:8025 2>/dev/null &
fi

log_success "Test terminé"