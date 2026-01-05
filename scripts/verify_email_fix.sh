#!/bin/bash

###############################################################################
# Script de vérification du correctif email Airflow 3.x
###############################################################################

set -e

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[⚠]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║     Vérification Correctif Email Airflow 3.x                  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_info "Vérification des fichiers mis à jour..."

# Vérifier que le fichier existe
if [ ! -f "dags/utils/amue_report_generator.py" ]; then
    log_error "Fichier dags/utils/amue_report_generator.py non trouvé"
    exit 1
fi

# Vérifier que le code utilise smtplib
if grep -q "import smtplib" dags/utils/amue_report_generator.py; then
    log_success "amue_report_generator.py utilise smtplib"
else
    log_warning "amue_report_generator.py n'utilise pas smtplib"
    log_info "Le fichier doit être mis à jour"
    exit 1
fi

# Vérifier que send_email d'Airflow n'est plus utilisé
if grep -q "from airflow.utils.email import send_email" dags/utils/amue_report_generator.py; then
    log_warning "amue_report_generator.py utilise encore airflow.utils.email"
    log_info "Le fichier doit être mis à jour"
    exit 1
else
    log_success "airflow.utils.email n'est plus utilisé"
fi

# Vérifier les variables SMTP
log_info "Vérification des variables SMTP..."

if [ -f "config/airflow_variables.json" ]; then
    if grep -q "smtp_host" config/airflow_variables.json; then
        log_success "Variables SMTP présentes dans airflow_variables.json"
    else
        log_warning "Variables SMTP absentes de airflow_variables.json"
        log_info "Elles seront créées avec les valeurs par défaut"
    fi
else
    log_error "Fichier config/airflow_variables.json non trouvé"
    exit 1
fi

# Test de connexion SMTP
log_info "Test de connexion SMTP..."

if $DOCKER_CMD ps | grep -q "mailhog.*Up"; then
    log_success "MailHog est en cours d'exécution"

    # Test connexion Python
    TEST_OUTPUT=$($DOCKER_CMD exec -T airflow-apiserver python3 << 'EOF' 2>&1
import smtplib
try:
    server = smtplib.SMTP('mailhog', 1025, timeout=5)
    server.quit()
    print("OK")
except Exception as e:
    print(f"ERROR: {e}")
EOF
)

    if echo "$TEST_OUTPUT" | grep -q "OK"; then
        log_success "Connexion SMTP à MailHog fonctionnelle"
    else
        log_error "Impossible de se connecter à MailHog"
        echo "$TEST_OUTPUT"
        exit 1
    fi
else
    log_error "MailHog n'est pas en cours d'exécution"
    log_info "Démarrez avec: ./manage.sh start"
    exit 1
fi

# Test d'envoi complet
log_info "Test d'envoi email..."

TEST_RESULT=$($DOCKER_CMD exec -T airflow-apiserver python3 << 'EOF' 2>&1
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

try:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Test Verification'
    msg['From'] = 'test@test.com'
    msg['To'] = 'admin@test.com'

    html = '<html><body><h1>Test OK</h1></body></html>'
    part = MIMEText(html, 'html')
    msg.attach(part)

    server = smtplib.SMTP('mailhog', 1025)
    server.sendmail('test@test.com', ['admin@test.com'], msg.as_string())
    server.quit()

    print("SUCCESS")
except Exception as e:
    print(f"ERROR: {e}")
EOF
)

if echo "$TEST_RESULT" | grep -q "SUCCESS"; then
    log_success "Envoi email test réussi"
else
    log_error "Échec envoi email test"
    echo "$TEST_RESULT"
    exit 1
fi

# Vérifier dans MailHog
if command -v curl &> /dev/null; then
    EMAIL_COUNT=$(curl -s http://localhost:8025/api/v2/messages 2>/dev/null | jq '.items | length' 2>/dev/null || echo "?")
    log_info "Emails dans MailHog: $EMAIL_COUNT"

    if [ "$EMAIL_COUNT" != "?" ] && [ "$EMAIL_COUNT" -gt 0 ]; then
        log_success "MailHog a reçu des emails"
    fi
fi

# Résumé
cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                        RÉSULTAT                               ║
╚═══════════════════════════════════════════════════════════════╝

EOF

log_success "Tous les tests sont passés !"

cat << EOF

Le correctif email Airflow 3.x est correctement appliqué.

Vous pouvez maintenant :
1. Tester avec: ./manage.sh test-email
2. Voir les emails: http://localhost:8025
3. Déclencher le DAG: ./manage.sh trigger amue_multi_table_import_v2

EOF

exit 0