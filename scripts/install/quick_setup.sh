#!/bin/bash

###############################################################################
# Script de setup rapide pour Airflow AMUE
# Configure l'environnement complet en une commande
# Les credentials ne sont JAMAIS écrits sur disque : ils sont saisis
# interactivement et injectés directement dans la base de données Airflow.
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
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        Configuration rapide Airflow AMUE                      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF

echo ""

###############################################################################
# Fonctions utilitaires
###############################################################################

ask() {
    local prompt="$1"
    local default="$2"
    local result

    if [[ -n "$default" ]]; then
        echo -n "$prompt [$default] : " >&2
        read -r result </dev/tty
        echo "${result:-$default}"
    else
        echo -n "$prompt : " >&2
        read -r result </dev/tty
        echo "$result"
    fi
}

ask_secret() {
    local prompt="$1"
    local default="$2"
    local result
    if [[ -n "$default" ]]; then
        echo -n "$prompt [$default] : " >&2
    else
        echo -n "$prompt : " >&2
    fi
    read -rs result </dev/tty
    echo "" >&2
    echo "${result:-$default}"
}

ask_choice() {
    local prompt="$1"
    local default="$2"
    local result

    # Affiche sur stderr pour que ce soit visible même dans une substitution de commande
    echo -e "${CYAN}$prompt${NC}" >&2
    echo "  1) dev (sandbox - pour les tests)" >&2
    echo "  2) prod (production)" >&2
    echo -n "Votre choix [$default] : " >&2
    read -r result </dev/tty
    echo "${result:-$default}"
}

ask_confirm() {
    local prompt="$1"
    local default="$2"
    local result
    echo -n "$prompt (o/N) : " >&2
    read -r result </dev/tty
    echo "${result:-$default}"
}

###############################################################################
# Étape 1: Vérification des prérequis
###############################################################################

log_info "Étape 1/9: Vérification des prérequis"

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
# Étape 2: Choix de l'environnement
###############################################################################

log_info "Étape 2/9: Choix de l'environnement"

echo ""
ENV_CHOICE=$(ask_choice "Quel environnement voulez-vous configurer ?" "1")

case "$ENV_CHOICE" in
    1|dev)
        ENVIRONMENT="dev"
        AMUE_API_HOST="https://sandbox.api.amue.fr"
        AMUE_AUTH_HOST="https://sandbox.auth.amue.fr"
        AMUE_TOKEN_URL="https://sandbox.auth.amue.fr/auth/fer/oauth/token"
        AMUE_API_ENV_PATH="preprod"
        SMTP_HOST="mailhog"
        SMTP_PORT="1025"
        log_info "Environnement: DEV (sandbox)"
        ;;
    2|prod)
        ENVIRONMENT="production"
        AMUE_API_HOST="https://api.amue.fr"
        AMUE_AUTH_HOST="https://auth.amue.fr"
        AMUE_TOKEN_URL="https://auth.amue.fr/auth/fer/oauth/token"
        AMUE_API_ENV_PATH="prod"
        SMTP_HOST=$(ask "Serveur SMTP" "smtp.example.com")
        SMTP_PORT=$(ask "Port SMTP" "587")
        log_info "Environnement: PRODUCTION"
        ;;
    *)
        log_error "Choix invalide"
        exit 1
        ;;
esac

echo ""

###############################################################################
# Étape 3: Création de la structure de dossiers
###############################################################################

log_info "Étape 3/9: Création de la structure"

cd "$PROJECT_DIR"

mkdir -p dags
mkdir -p logs
mkdir -p plugins
mkdir -p config/exports
mkdir -p scripts

log_success "Structure créée"

###############################################################################
# Étape 4: Configuration des paramètres
###############################################################################

log_info "Étape 4/9: Configuration des paramètres"

echo ""
log_info "=== Configuration générale ==="
UNIVERSITE=$(ask "Code université (ex: ul pour Lorraine)" "ul")
AMUE_REPORT_RECIPIENTS=$(ask "Emails destinataires des rapports (séparés par des virgules)" "admin@example.com")
SMTP_MAIL_FROM=$(ask "Adresse d'envoi des emails" "airflow@amue-project.local")

echo ""
log_info "=== Tables AMUE / ECC ==="
log_info "Les tables sont pré-configurées dans splus_admin.amue_tables (init_db.sql)."
log_info "Pour ajouter/modifier des tables après le setup : ./manage.sh add-table"

echo ""
log_info "=== Credentials AMUE API (stockés dans Airflow DB uniquement) ==="
OAUTH_CLIENT_ID=$(ask "OAuth Client ID" "")
OAUTH_CLIENT_SECRET=$(ask_secret "OAuth Client Secret")

echo ""
log_info "=== Credentials PostgreSQL (stockés dans Airflow DB uniquement) ==="
PG_HOST=$(ask "PostgreSQL host" "postgres-data")
PG_DATABASE=$(ask "PostgreSQL database" "business_data")
PG_SCHEMA=$(ask "PostgreSQL schema" "splus")
PG_PORT=$(ask "PostgreSQL port" "5432")
PG_LOGIN=$(ask "PostgreSQL login" "datauser")
PG_PASSWORD=$(ask_secret "PostgreSQL password" "datapass")

echo ""
log_info "=== Connexion Oracle ECC (optionnel) ==="
log_info "Requis uniquement si vous utilisez le DAG ECC (tables lfa1, lfb1, pa0001, pa0002...)"
USE_ECC=$(ask_confirm "Configurer la connexion Oracle ECC ?" "n")
ORACLE_HOST=""
ORACLE_PORT="1521"
ORACLE_SID=""
ORACLE_SCHEMA=""
ORACLE_LOGIN=""
ORACLE_PASSWORD=""
if [[ "${USE_ECC,,}" =~ ^[oOyY]$ ]]; then
    ORACLE_HOST=$(ask "Hôte Oracle" "oracle.example.fr")
    ORACLE_PORT=$(ask "Port Oracle" "1521")
    ORACLE_SID=$(ask "SID / Service Oracle" "SAPSR3")
    ORACLE_SCHEMA=$(ask "Schéma Oracle (ex: sapsr3)" "sapsr3")
    ORACLE_LOGIN=$(ask "Login Oracle" "")
    ORACLE_PASSWORD=$(ask_secret "Mot de passe Oracle")
fi

###############################################################################
# Étape 5: Génération des fichiers de configuration
###############################################################################

log_info "Étape 5/9: Génération des fichiers de configuration"

# Génération de la clé Fernet
generate_fernet_key() {
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
    openssl rand -base64 32
}

# Préserver la clé Fernet existante si .env est déjà présent
FERNET_KEY=""
if [[ -f ".env" ]]; then
    FERNET_KEY=$(grep "^AIRFLOW__CORE__FERNET_KEY=" ".env" | cut -d'=' -f2-)
fi
if [[ -z "$FERNET_KEY" ]]; then
    FERNET_KEY=$(generate_fernet_key)
    log_info "Nouvelle clé Fernet générée"
else
    log_info "Clé Fernet existante préservée"
fi

# Création du fichier .env (sans credentials)
cat > ".env" << EOFENV
###############################################################################
# Configuration Airflow AMUE
# ATTENTION: Ce fichier contient des secrets de configuration, ne pas le commiter !
# Les credentials des connexions (OAuth, PostgreSQL, Oracle) sont stockés
# directement dans la base de données Airflow (chiffrés) et n'apparaissent pas ici.
###############################################################################

# Airflow
AIRFLOW_UID=1001
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.7
AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS="requests oauthlib requests-oauthlib oracledb"

# SMTP
SMTP_HOST=$SMTP_HOST
SMTP_PORT=$SMTP_PORT
EOFENV

log_success "Fichier .env créé (sans credentials — stockés dans Airflow DB)"

# Création du fichier de variables (sans secrets)
cat > "config/airflow_variables.json" << EOFVARS
{
  "environment": "$ENVIRONMENT",
  "oauth_api_connection_id": "oauth_api",
  "universite": "$UNIVERSITE",
  "api_endpoint_admin": "finances/cdv/v1/$AMUE_API_ENV_PATH/\${univ}/admin",
  "api_endpoint_table": "finances/cdv/v1/$AMUE_API_ENV_PATH/\${univ}/table",
  "amue_import_batch_size": "5000",
  "amue_max_history_days": "7",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "2",
  "amue_report_recipients": "$AMUE_REPORT_RECIPIENTS",
  "smtp_host": "$SMTP_HOST",
  "smtp_port": "$SMTP_PORT",
  "smtp_mail_from": "$SMTP_MAIL_FROM",
  "amue_last_successful_run": "",
  "last_import_report": "",
  "TYPE_MAPPING_SQLITE_TO_POSTGRES": {
    "TEXT": "TEXT",
    "CHAR": "CHAR",
    "VARCHAR": "VARCHAR",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "REAL": "DOUBLE PRECISION",
    "NUMERIC": "NUMERIC",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP",
    "BLOB": "BYTEA"
  }
}
EOFVARS

log_success "Fichier config/airflow_variables.json créé (sans secrets)"

###############################################################################
# Étape 6: Démarrage des containers
###############################################################################

log_info "Étape 6/9: Démarrage des containers Docker"

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
# Étape 7: Configuration d'Airflow
###############################################################################

log_info "Étape 7/9: Configuration d'Airflow"

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

# Lancer le script de configuration (variables uniquement)
log_info "Configuration des variables Airflow..."
chmod +x "$SCRIPT_DIR/setup_airflow_config.sh"
"$SCRIPT_DIR/setup_airflow_config.sh" --variables-only

# Injection directe des credentials dans Airflow (jamais sur disque)
log_info "Injection des credentials dans les connexions Airflow..."

$DOCKER_CMD exec -T airflow-apiserver airflow connections delete oauth_api 2>/dev/null || true
if ! $DOCKER_CMD exec -T airflow-apiserver airflow connections add oauth_api \
    --conn-type http \
    --conn-host "$AMUE_API_HOST" \
    --conn-login "$OAUTH_CLIENT_ID" \
    --conn-password "$OAUTH_CLIENT_SECRET" \
    --conn-extra "{\"token_url\": \"$AMUE_TOKEN_URL\", \"api_base_url\": \"$AMUE_API_HOST\"}"; then
    log_error "Échec création connexion oauth_api"
fi

$DOCKER_CMD exec -T airflow-apiserver airflow connections delete postgres_data 2>/dev/null || true
if ! $DOCKER_CMD exec -T airflow-apiserver airflow connections add postgres_data \
    --conn-type postgres \
    --conn-host "$PG_HOST" \
    --conn-schema "$PG_DATABASE" \
    --conn-port "$PG_PORT" \
    --conn-login "$PG_LOGIN" \
    --conn-password "$PG_PASSWORD" \
    --conn-extra "{\"options\": \"-c search_path=$PG_SCHEMA\"}"; then
    log_error "Échec création connexion postgres_data"
fi

if [[ -n "$ORACLE_HOST" ]]; then
    $DOCKER_CMD exec -T airflow-apiserver airflow connections delete oracle_data 2>/dev/null || true
    if ! $DOCKER_CMD exec -T airflow-apiserver airflow connections add oracle_data \
        --conn-type odbc \
        --conn-host "$ORACLE_HOST" \
        --conn-schema "$ORACLE_SID" \
        --conn-port "$ORACLE_PORT" \
        --conn-login "$ORACLE_LOGIN" \
        --conn-password "$ORACLE_PASSWORD"; then
        log_error "Échec création connexion oracle_data"
    fi
fi

log_success "Credentials injectés directement dans Airflow DB"

log_success "Configuration Airflow terminée"

###############################################################################
# Étape 8/9: Configuration des tables à importer
###############################################################################

log_info "Étape 8/9: Configuration des tables à importer"

echo ""
log_info "Saisissez les tables AMUE à importer (table_name, primary_key, delta)."
log_info "Vous pourrez en ajouter/modifier plus tard via : ./manage.sh load-tables"
echo ""

TABLE_COUNT=0
while true; do
    echo -n "Ajouter une table ? (o/N) : " >&2
    read -r ADD_TABLE </dev/tty
    [[ ! "$ADD_TABLE" =~ ^[oOyY]$ ]] && break

    echo -n "  Nom de la table : " >&2
    read -r T_NAME </dev/tty
    [[ -z "$T_NAME" ]] && continue

    echo -n "  Clé primaire (séparée par virgules, ex: MANDT,BUKRS,BELNR) : " >&2
    read -r T_PK </dev/tty

    echo -n "  Colonne delta (laisser vide si import complet) : " >&2
    read -r T_DELTA </dev/tty

    $DOCKER_CMD exec -T postgres-data psql -U "$PG_LOGIN" -d "$PG_DATABASE" -q -c \
        "INSERT INTO splus_admin.amue_tables (table_name, primary_key, delta)
         VALUES ('$T_NAME', '$T_PK', '$T_DELTA')
         ON CONFLICT (table_name) DO UPDATE
           SET primary_key = EXCLUDED.primary_key,
               delta       = EXCLUDED.delta,
               updated_at  = NOW();" \
    && log_success "  Table '$T_NAME' enregistrée" \
    || log_warning  "  Erreur pour '$T_NAME'"

    ((TABLE_COUNT+=1))
    echo ""
done

if [[ $TABLE_COUNT -gt 0 ]]; then
    log_success "$TABLE_COUNT table(s) configurée(s)"
    echo ""
    log_info "Tables enregistrées :"
    $DOCKER_CMD exec -T postgres-data psql -U "$PG_LOGIN" -d "$PG_DATABASE" -c \
        "SELECT table_name, primary_key, delta, enabled FROM splus_admin.amue_tables ORDER BY table_name;"
else
    log_info "Aucune table ajoutée. Utilisez ./manage.sh load-tables plus tard."
fi

###############################################################################
# Étape 9/9: Configuration des utilisateurs
###############################################################################

log_info "Étape 9/9: Configuration des utilisateurs"

echo ""
log_info "=== Configuration des utilisateurs Airflow ==="
log_info "L'utilisateur admin par défaut (airflow/airflow) a été créé."
log_info "Voulez-vous créer des utilisateurs supplémentaires ?"
echo ""

AIRFLOW_USERS="[]"

while true; do
    echo -n "Créer un utilisateur supplémentaire ? (o/N) : " >&2
    read -r CREATE_USER </dev/tty
    [[ ! "$CREATE_USER" =~ ^[oOyY]$ ]] && break

    echo -n "  Username : " >&2
    read -r USER_NAME </dev/tty
    [[ -z "$USER_NAME" ]] && continue

    echo -n "  Email : " >&2
    read -r USER_EMAIL </dev/tty
    [[ -z "$USER_EMAIL" ]] && USER_EMAIL="${USER_NAME}@local"

    echo -n "  Prénom [$USER_NAME] : " >&2
    read -r USER_FIRSTNAME </dev/tty
    [[ -z "$USER_FIRSTNAME" ]] && USER_FIRSTNAME="$USER_NAME"

    echo -n "  Nom [User] : " >&2
    read -r USER_LASTNAME </dev/tty
    [[ -z "$USER_LASTNAME" ]] && USER_LASTNAME="User"

    echo "  Rôles: Admin, Op, User, Viewer" >&2
    echo -n "  Rôle [Viewer] : " >&2
    read -r USER_ROLE </dev/tty
    [[ -z "$USER_ROLE" ]] && USER_ROLE="Viewer"

    echo -n "  Mot de passe : " >&2
    read -rs USER_PASSWORD </dev/tty
    echo "" >&2
    [[ -z "$USER_PASSWORD" ]] && { log_warning "Mot de passe vide, utilisateur ignoré"; continue; }

    log_info "Création de l'utilisateur $USER_NAME ($USER_ROLE)..."

    $DOCKER_CMD exec -T airflow-apiserver airflow users create \
        --username "$USER_NAME" \
        --email "$USER_EMAIL" \
        --firstname "$USER_FIRSTNAME" \
        --lastname "$USER_LASTNAME" \
        --role "$USER_ROLE" \
        --password "$USER_PASSWORD" 2>/dev/null && \
        log_success "Utilisateur $USER_NAME créé" || \
        log_warning "Erreur lors de la création de $USER_NAME"

    echo ""
done

log_info "Liste des utilisateurs:"
$DOCKER_CMD exec -T airflow-apiserver airflow users list

###############################################################################
# Vérification finale
###############################################################################

log_info "Vérification finale..."

echo ""
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

EOF

if [[ "$ENVIRONMENT" == "dev" ]]; then
    echo -e "${GREEN}Environnement: DEV (sandbox)${NC}"
    echo -e "   API Host: $AMUE_API_HOST"
    echo -e "   Endpoints: finances/cdv/v1/preprod/..."
else
    echo -e "${YELLOW}Environnement: PRODUCTION${NC}"
    echo -e "   API Host: $AMUE_API_HOST"
    echo -e "   Endpoints: finances/cdv/v1/prod/..."
fi

cat << EOF

📍 URLs importantes:
   - Interface Web Airflow: http://localhost:8080
   - Interface Emails (dev): http://localhost:8025
   - Base Airflow:  localhost:5432
   - Base Données:  localhost:5433

🔐 Credentials par défaut:
   - Username: airflow
   - Password: airflow

🔒 Sécurité:
   - Les credentials sont stockés UNIQUEMENT dans la base de données Airflow (chiffrés)
   - Le fichier .env et les fichiers JSON ne contiennent PAS de credentials
   - Ne commitez JAMAIS le fichier .env (contient la clé Fernet)

⚙️ Commandes utiles:
   - Logs:              ./manage.sh logs airflow-scheduler
   - Arrêter:           ./manage.sh stop
   - Redémarrer:        ./manage.sh restart
   - Reconfig:          ./manage.sh config
   - Vérifier config:   ./manage.sh verify

EOF

log_success "Setup terminé avec succès!"
