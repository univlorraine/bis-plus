#!/bin/bash

###############################################################################
# Script de setup rapide pour Airflow AMUE
# Configure l'environnement complet en une commande
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

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        Configuration rapide Airflow AMUE                      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF

echo ""

###############################################################################
# Étape 1: Vérification des prérequis
###############################################################################

log_info "Étape 1/6: Vérification des prérequis"

if ! command -v docker &> /dev/null; then
    log_error "Docker n'est pas installé"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    log_error "Docker Compose n'est pas installé"
    exit 1
fi

DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

if ! command -v jq &> /dev/null; then
    log_warning "jq n'est pas installé, tentative d'installation..."
    if command -v apt-get &> /dev/null; then
        sudo apt-get update && sudo apt-get install -y jq
    elif command -v yum &> /dev/null; then
        sudo yum install -y jq
    elif command -v brew &> /dev/null; then
        brew install jq
    else
        log_error "Impossible d'installer jq automatiquement. Installez-le manuellement."
        exit 1
    fi
fi

log_success "Prérequis OK"

###############################################################################
# Étape 2: Création de la structure de dossiers
###############################################################################

log_info "Étape 2/6: Création de la structure"

cd "$PROJECT_DIR"

mkdir -p dags/utils
mkdir -p logs
mkdir -p plugins
mkdir -p config/exports
mkdir -p scripts

log_success "Structure créée"

###############################################################################
# Étape 3: Vérification des fichiers de configuration
###############################################################################

log_info "Étape 3/6: Vérification des fichiers de configuration"

ask() {
    local prompt="$1"
    local default="$2"
    local result

    if [[ -n "$default" ]]; then
        read -rp "$prompt [$default] : " result
        echo "${result:-$default}"
    else
        read -rp "$prompt : " result
        echo "$result"
    fi
}

ask_secret() {
    local prompt="$1"
    local result
    read -rsp "$prompt : " result
    echo "$result"
}

log_info "Configuration interactive des variables Airflow"

UNIVERSITE=$(ask "Nom de l'université" "")
AMUE_REPORT_RECIPIENTS=$(ask "Emails destinataires du rapport (séparés par des virgules)" "admin@example.com")
SMTP_MAIL_FROM=$(ask "Adresse d'envoi SMTP" "airflow@amue-project.local")

log_info "Configuration des tables AMUE à importer"

TABLES_JSON="[]"

while true; do
    TABLE_NAME=$(ask "Nom de la table AMUE (vide pour terminer)" "")
    [[ -z "$TABLE_NAME" ]] && break

    TABLE_DELTA=$(ask "Colonne delta pour $TABLE_NAME" "")

    TABLES_JSON=$(echo "$TABLES_JSON" | jq \
        --arg name "$TABLE_NAME" \
        --arg delta "$TABLE_DELTA" \
        '. + [{
            name: $name,
            primary_key: "",
            delta: $delta,
            last_import: "",
            finger_print: ""
        }]')
done

cat > config/airflow_variables.json << EOFVARS
{
  "environment": "dev",
  "oauth_api_connection_id": "oauth_api",
  "universite": "$UNIVERSITE",
  "api_endpoint_admin": "finances/cdv/v1/preprod/\${univ}/admin",
  "api_endpoint_table": "finances/cdv/v1/preprod/\${univ}/table",
  "amue_max_history_days": "7",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "2",
  "amue_report_recipients": "$AMUE_REPORT_RECIPIENTS",
  "smtp_host": "mailhog",
  "smtp_port": "1025",
  "smtp_mail_from": "$SMTP_MAIL_FROM",
  "amue_tables_to_import": $TABLES_JSON,
  "amue_last_successful_run": "",
  "last_import_report": ""
}
EOFVARS

log_info "Configuration interactive des connexions"

OAUTH_LOGIN=$(ask "OAuth client_id" "")
OAUTH_PASSWORD=$(ask_secret "OAuth client_secret")
echo

PG_HOST=$(ask "PostgreSQL host" "postgres-data")
PG_DATABASE=$(ask "PostgreSQL database" "business_data")
PG_SCHEMA=$(ask "PostgreSQL schema" "public")
PG_LOGIN=$(ask "PostgreSQL login" "datauser")
PG_PASSWORD=$(ask_secret "PostgreSQL password")
PG_PORT=$(ask "PostgreSQL port" "5432")
echo

cat > config/airflow_connections.json << EOFCONNS
{
  "oauth_api": {
    "conn_type": "http",
    "host": "https://sandbox.api.amue.fr",
    "login": "$OAUTH_LOGIN",
    "password": "$OAUTH_PASSWORD",
    "extra": {
      "token_url": "https://sandbox.auth.amue.fr/auth/fer/oauth/token",
      "api_base_url": "https://sandbox.api.amue.fr"
    }
  },
  "postgres_data": {
    "conn_type": "postgres",
    "host": "$PG_HOST",
    "schema": "$PG_DATABASE",
    "login": "$PG_LOGIN",
    "password": "$PG_PASSWORD",
    "port": $PG_PORT,
    "extra": {
      "options": "-c search_path=$PG_SCHEMA"
    }
  }
}
EOFCONNS

ENV_FILE=".env"

generate_fernet_key() {
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
}

if [[ ! -f "$ENV_FILE" ]]; then
    log_info "Création du fichier .env..."

    cat > "$ENV_FILE" << EOFENV
AIRFLOW_UID=1001
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3
AIRFLOW__CORE__FERNET_KEY=$(generate_fernet_key)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=requests oauthlib requests-oauthlib
EOFENV

else
    if ! grep -q '^AIRFLOW__CORE__FERNET_KEY=' "$ENV_FILE"; then
        log_info "FERNET_KEY absente → génération..."

        echo "AIRFLOW__CORE__FERNET_KEY=$(generate_fernet_key)" >> "$ENV_FILE"
    else
        log_info "FERNET_KEY déjà présente → aucune action"
    fi
fi

log_success "Fichiers de configuration OK"

###############################################################################
# Étape 4: Démarrage des containers
###############################################################################

log_info "Étape 4/6: Démarrage des containers Docker"

log_info "Arrêt des containers existants..."
$DOCKER_CMD down -v 2>/dev/null || true

log_info "Démarrage des containers (cela peut prendre quelques minutes)..."
$DOCKER_CMD up -d

log_info "Attente que les services soient prêts..."
sleep 30

# Vérification que les services sont en cours d'exécution
if ! $DOCKER_CMD ps | grep -q "airflow-apiserver"; then
    log_error "Le service airflow-apiserver n'a pas démarré correctement"
    log_info "Logs:"
    $DOCKER_CMD logs airflow-apiserver --tail=50
    exit 1
fi

log_success "Containers démarrés"

###############################################################################
# Étape 5: Configuration d'Airflow
###############################################################################

log_info "Étape 5/6: Configuration d'Airflow"

# Attendre que l'API soit disponible
log_info "Attente de l'API Airflow..."
for i in {1..30}; do
    if curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
        log_success "API Airflow disponible"
        break
    fi
    if [[ $i -eq 30 ]]; then
        log_error "Timeout: API Airflow non disponible"
        exit 1
    fi
    sleep 5
done

# Lancer le script de configuration
log_info "Configuration des variables et connexions..."
chmod +x "$SCRIPT_DIR/setup_airflow_config.sh"
"$SCRIPT_DIR/setup_airflow_config.sh" --external

log_success "Configuration Airflow terminée"

###############################################################################
# Étape 6: Vérification finale
###############################################################################

log_info "Étape 6/6: Vérification finale"

log_info "Vérification des variables..."
$DOCKER_CMD exec -T airflow-apiserver airflow variables list | head -20

echo ""
log_info "Vérification des connexions..."
$DOCKER_CMD exec -T airflow-apiserver airflow connections list

echo ""
log_info "État des containers..."
$DOCKER_CMD ps

###############################################################################
# Résumé final
###############################################################################

cat << EOF

╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                    Configuration terminée!                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝

🎉 Airflow est maintenant configuré et prêt à l'emploi!

📍 URLs importantes:
   - Interface Web: http://localhost:8080
   - Base Airflow:  localhost:5432
   - Base Données:  localhost:5433

🔐 Credentials par défaut:
   - Username: airflow
   - Password: airflow

📝 Prochaines étapes:
   1. Modifiez config/airflow_connections.json avec vos vraies credentials
   2. Exécutez: ./manage.sh config
   3. Accédez à http://localhost:8080
   4. Activez le DAG amue_multi_table_import

⚙️ Commandes utiles:
   - Logs:              ./manage.sh logs airflow-scheduler
   - Arrêter:           ./manage.sh stop
   - Redémarrer:        ./manage.sh restart
   - Reconfig:          ./manage.sh config
   - Vérifier config:   ./manage.sh verify
   - Exporter config:   ./manage.sh export

EOF

log_success "Setup terminé avec succès!"