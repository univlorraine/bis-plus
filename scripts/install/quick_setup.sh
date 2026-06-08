#!/bin/bash

###############################################################################
# Script de setup rapide pour Airflow AMUE
# Configure l'environnement complet en une commande
# Les credentials ne sont JAMAIS écrits sur disque : ils sont saisis
# interactivement et injectés directement dans la base de données Airflow.
###############################################################################

set -e
set -o pipefail

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

trap 'log_error "Setup interrompu à la ligne $LINENO. Vérifiez : $DOCKER_CMD logs airflow-apiserver --tail=50"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Détecté tôt — requis par le trap ERR avant l'étape 1
DOCKER_CMD="docker-compose"
if ! command -v docker-compose &> /dev/null; then
    DOCKER_CMD="docker compose"
fi

cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        Configuration rapide Airflow AMUE                      ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF

echo ""
echo -e "\033[1;33m╔═══════════════════════════════════════════════════════════════╗\033[0m"
echo -e "\033[1;33m║  ATTENTION : ce script supprime tous les volumes Docker        ║\033[0m"
echo -e "\033[1;33m║  (docker-compose down -v). Toutes les données existantes       ║\033[0m"
echo -e "\033[1;33m║  (PostgreSQL, logs) seront DÉFINITIVEMENT PERDUES.             ║\033[0m"
echo -e "\033[1;33m╚═══════════════════════════════════════════════════════════════╝\033[0m"
echo ""
echo -n "Continuer ? (o/N) : "
read -r _CONFIRM </dev/tty
if [[ ! "$_CONFIRM" =~ ^[oOyY]$ ]]; then
    echo "Annulé."
    exit 0
fi
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
    local default="${2:-n}"
    local result
    if [[ "${default,,}" =~ ^[oy]$ ]]; then
        echo -n "$prompt (O/n) : " >&2
    else
        echo -n "$prompt (o/N) : " >&2
    fi
    read -r result </dev/tty
    echo "${result:-$default}"
}

validate_password_strength() {
    local pwd="$1" label="${2:-Mot de passe}"
    if [[ ${#pwd} -lt 12 ]]; then
        log_error "$label trop court (minimum 12 caractères)"
        return 1
    fi
    if [[ ! "$pwd" =~ [A-Z] ]]; then
        log_error "$label doit contenir au moins une majuscule"
        return 1
    fi
    if [[ ! "$pwd" =~ [0-9] ]]; then
        log_error "$label doit contenir au moins un chiffre"
        return 1
    fi
    return 0
}

airflow_exec() {
    $DOCKER_CMD exec -T airflow-apiserver airflow "$@" 2>&1 \
        | { grep -v "\[alembic\.runtime\.plugins\]" || true; }
    return ${PIPESTATUS[0]}
}

###############################################################################
# Étape 1: Vérification des prérequis
###############################################################################

log_info "Étape 1/9: Vérification des prérequis"

if ! command -v docker &> /dev/null; then
    log_error "Docker n'est pas installé"
    exit 1
fi

if ! $DOCKER_CMD version &> /dev/null; then
    log_error "Docker Compose n'est pas installé ou inaccessible"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    log_error "python3 est requis (génération clé Fernet, parsing JSON)"
    exit 1
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
        AIRFLOW_ENV_VALUE="dev"
        AMUE_API_HOST="https://sandbox.api.amue.fr"
        AMUE_TOKEN_URL="https://sandbox.auth.amue.fr/auth/fer/oauth/token"
        AMUE_API_ENV_PATH="preprod"
        SMTP_HOST="mailhog"
        SMTP_PORT="1025"
        log_info "Environnement: DEV (sandbox)"
        ;;
    2|prod)
        ENVIRONMENT="production"
        AIRFLOW_ENV_VALUE="prod"
        AMUE_API_HOST="https://api.amue.fr"
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
UNIVERSITE=$(ask "Code université (ex: univ)" "univ")
if [[ ! "$UNIVERSITE" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    log_error "Code université invalide — caractères autorisés : lettres, chiffres, - et _"
    exit 1
fi

while true; do
    AMUE_REPORT_RECIPIENTS=$(ask "Emails destinataires des rapports (séparés par des virgules)" "admin@example.com")
    if [[ "$AMUE_REPORT_RECIPIENTS" =~ ^[^@[:space:]]+@[^[:space:]]+(,[^@[:space:]]+@[^[:space:]]+)*$ ]]; then
        break
    fi
    log_error "Format d'email invalide. Exemple: user@domain.com,other@domain.com"
done
SMTP_MAIL_FROM=$(ask "Adresse d'envoi des emails" "airflow@amue.local")

echo ""
log_info "=== Tables AMUE / ECC ==="
log_info "Les tables sont pré-configurées dans splus_admin.amue_tables (init_db.sql)."
log_info "Pour ajouter/modifier des tables après le setup : ./manage.sh add-table"

echo ""
log_info "=== Authentification CAS (protocole CAS classique) ==="
CAS_SERVER_URL=""
CAS_VERSION="2"
CAS_DEFAULT_ROLE="Viewer"
CAS_SERVICE_URL=""
CAS_ALLOWED_USERS=""
CAS_ADMIN_USERS=""
AIRFLOW_ADMIN_USERNAME="airflow"
AIRFLOW_ADMIN_PASSWORD=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || openssl rand -base64 32 | tr -d '\n')

_USE_CAS=$(ask_confirm "Voulez-vous activer l'authentification CAS ?" "o")
if [[ ! "${_USE_CAS,,}" =~ ^[oy]$ ]]; then
    log_warning "CAS désactivé — fallback AUTH_DB actif (dev uniquement). Ne jamais utiliser en production."
else
    # ── Validation du serveur CAS (Option B) ──────────────────────────────────
    log_info "Une seule URL suffit — demandez-la à votre DSI."
    while true; do
        CAS_SERVER_URL=$(ask "URL du serveur CAS (ex: https://cas.votre-universite.fr)" "")
        if [[ -z "$CAS_SERVER_URL" ]]; then
            log_error "L'URL du serveur CAS est obligatoire."
            continue
        fi
        # Appel /serviceValidate avec un faux ticket — un vrai CAS répond toujours
        # avec du XML <cas:serviceResponse>, même pour un ticket invalide.
        echo -n "  Vérification du serveur CAS..." >&2
        _CAS_RESP=$(curl -s --max-time 10 \
            "${CAS_SERVER_URL%/}/serviceValidate?ticket=ST-FAKE-SETUP-CHECK&service=http://localhost" \
            2>/dev/null)
        if echo "$_CAS_RESP" | grep -q "<cas:serviceResponse"; then
            log_success "Serveur CAS valide (endpoint serviceValidate opérationnel)"
            break
        else
            _HTTP=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
                "${CAS_SERVER_URL%/}/login" 2>/dev/null)
            if [[ "$_HTTP" =~ ^(200|302|301)$ ]]; then
                log_warning "Le serveur répond (HTTP $_HTTP) mais l'endpoint /serviceValidate n'a pas retourné de XML CAS."
                log_warning "Cela peut indiquer une URL incorrecte ou un chemin de contexte différent."
                _FORCE=$(ask_confirm "Continuer quand même avec cette URL ?" "n")
                [[ "${_FORCE,,}" =~ ^[oy]$ ]] && break
            else
                log_error "Impossible de joindre le serveur CAS (HTTP ${_HTTP:-timeout}). Vérifiez l'URL."
            fi
        fi
    done

    CAS_VERSION=$(ask "Version du protocole CAS" "2")
    echo "Rôles disponibles : Admin, Op, User, Viewer" >&2
    CAS_DEFAULT_ROLE=$(ask "Rôle attribué à la première connexion" "Viewer")
    CAS_SERVICE_URL=$(ask "URL de callback (vide = auto-détection)" "")
    CAS_ALLOWED_USERS=$(ask "Usernames autorisés à se connecter, séparés par des virgules (vide = tous)" "")

    # ── Validation des admins CAS (format + confirmation) ─────────────────────
    echo ""
    log_info "=== Administrateurs Airflow (rôle Admin) ==="
    log_info "Seuls ces usernames CAS pourront avoir le rôle Admin."
    log_info "Tout autre utilisateur sera bloqué au rôle '${CAS_DEFAULT_ROLE}'."
    while true; do
        CAS_ADMIN_USERS=$(ask "Usernames Admin CAS, séparés par des virgules (obligatoire)" "")
        if [[ -z "$CAS_ADMIN_USERS" ]]; then
            log_error "Au moins un username Admin CAS est requis."
            continue
        fi

        # Validation de format : lettres, chiffres, point, tiret, underscore uniquement
        _format_ok=true
        for _u in ${CAS_ADMIN_USERS//,/ }; do
            _u="${_u// /}"
            if [[ ! "$_u" =~ ^[a-zA-Z0-9._-]+$ ]]; then
                log_error "Username invalide : '$_u' — seuls les caractères a-z A-Z 0-9 . - _ sont autorisés."
                _format_ok=false
            fi
        done
        [[ "$_format_ok" == false ]] && continue

        # Confirmation explicite
        echo "" >&2
        log_info "Récapitulatif des administrateurs CAS :"
        for _u in ${CAS_ADMIN_USERS//,/ }; do
            echo "    • ${_u// /}" >&2
        done
        echo "" >&2
        log_warning "Ces usernames doivent correspondre à des comptes CAS existants."
        log_warning "Aucune vérification d'existence n'est possible sans connexion réelle."
        _CONFIRM=$(ask_confirm "Confirmez-vous ces usernames Admin ?" "o")
        if [[ "${_CONFIRM,,}" =~ ^[oy]$ ]]; then
            break
        fi
        log_info "Resaisissez les usernames Admin."
    done

    echo ""
    log_info "=== Utilisateur de bootstrap Airflow ==="
    log_info "Compte technique créé au démarrage (accessible via AUTH_DB uniquement, pas via CAS)."
    AIRFLOW_ADMIN_USERNAME=$(ask "Username de bootstrap" "airflow")
    if [[ -z "$AIRFLOW_ADMIN_USERNAME" ]]; then
        AIRFLOW_ADMIN_USERNAME="airflow"
    fi

    # Ajouter automatiquement les admins CAS à la whitelist de connexion
    if [[ -n "$CAS_ADMIN_USERS" ]]; then
        for _admin in ${CAS_ADMIN_USERS//,/ }; do
            _admin="${_admin// /}"
            if [[ -z "$CAS_ALLOWED_USERS" ]]; then
                CAS_ALLOWED_USERS="$_admin"
            elif [[ "$CAS_ALLOWED_USERS" != *"$_admin"* ]]; then
                CAS_ALLOWED_USERS="$_admin,$CAS_ALLOWED_USERS"
            fi
        done
        log_info "Admins CAS ajoutés à CAS_ALLOWED_USERS : $CAS_ADMIN_USERS"
    fi
fi

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
    ORACLE_SID=$(ask "T91 / Service Oracle" "t91")
    ORACLE_SCHEMA=$(ask "Schéma Oracle (ex: sapsr3)" "sapsr3")
    ORACLE_LOGIN=$(ask "Login Oracle" "")
    ORACLE_PASSWORD=$(ask_secret "Mot de passe Oracle")
fi

###############################################################################
# Étape 5: Génération des fichiers de configuration
###############################################################################

log_info "Étape 5/9: Génération des fichiers de configuration"

# Les dépendances sont gérées via requirements.txt + image Docker (./manage.sh build).
# _PIP_ADDITIONAL_REQUIREMENTS est laissé vide pour ne pas réinstaller à chaque démarrage.
PIP_REQS=""

# Lecture de la version Airflow depuis docker-compose.yml pour rester synchronisé
AIRFLOW_IMAGE_NAME=$(grep 'AIRFLOW_IMAGE_NAME:-' "$PROJECT_DIR/docker-compose.yml" \
    | grep -oP '(?<=:-)[^}]+' | head -1 || echo "apache/airflow:3.1.7")

# Génération de la clé Fernet
generate_fernet_key() {
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
    python3 -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())" 2>/dev/null || \
    openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'
}

# Génération d'une secret key Flask aléatoire (signe les cookies de session Airflow)
generate_secret_key() {
    python3 -c "import secrets; print(secrets.token_hex(64))" 2>/dev/null || \
    openssl rand -hex 64
}

# Nouvelle clé à chaque setup (down -v supprime les volumes — l'ancienne clé est inutile)
FERNET_KEY=$(generate_fernet_key)
log_info "Clé Fernet générée"
SECRET_KEY=$(generate_secret_key)
log_info "Secret key Flask générée"

# Création du fichier .env (sans credentials de connexion)
cat > ".env" << EOFENV
###############################################################################
# Configuration Airflow AMUE
# ATTENTION: Ce fichier contient des secrets de configuration, ne pas le commiter !
# Les credentials des connexions (OAuth, PostgreSQL, Oracle) sont stockés
# directement dans la base de données Airflow (chiffrés) et n'apparaissent pas ici.
###############################################################################

# Airflow
AIRFLOW_UID=1001
AIRFLOW_IMAGE_NAME=$AIRFLOW_IMAGE_NAME
AIRFLOW__CORE__FERNET_KEY=$FERNET_KEY
AIRFLOW__API__SECRET_KEY=$SECRET_KEY

_AIRFLOW_WWW_USER_USERNAME=$AIRFLOW_ADMIN_USERNAME
_AIRFLOW_WWW_USER_PASSWORD=$AIRFLOW_ADMIN_PASSWORD
_PIP_ADDITIONAL_REQUIREMENTS="$PIP_REQS"

# Environnement de déploiement (conditionne TrustServerCertificate MSSQL et d'autres comportements)
AIRFLOW_ENV=$AIRFLOW_ENV_VALUE

# Authentification CAS classique (protocole ticket CAS v2/v3)
CAS_SERVER_URL=$CAS_SERVER_URL
CAS_VERSION=$CAS_VERSION
CAS_DEFAULT_ROLE=$CAS_DEFAULT_ROLE
CAS_SERVICE_URL=$CAS_SERVICE_URL
CAS_ALLOWED_USERS=$CAS_ALLOWED_USERS
CAS_ADMIN_USERS=$CAS_ADMIN_USERS

# SMTP
SMTP_HOST=$SMTP_HOST
SMTP_PORT=$SMTP_PORT

# Base de données métier (utilisé par docker-compose pour initialiser le container postgres-data)
PG_DATA_USER=$PG_LOGIN
PG_DATA_PASSWORD=$PG_PASSWORD
PG_DATA_DB=$PG_DATABASE
PG_DATA_PORT=5433
EOFENV

log_success "Fichier .env créé (sans credentials de connexion — stockés dans Airflow DB)"

# Calcul des endpoints selon l'environnement (prod : pas de segment de chemin intermédiaire)
if [[ "$ENVIRONMENT" == "production" ]]; then
    API_ENDPOINT_ADMIN='finances/cdv/v1/${univ}/admin'
    API_ENDPOINT_TABLE='finances/cdv/v1/${univ}/table'
else
    API_ENDPOINT_ADMIN="finances/cdv/v1/$AMUE_API_ENV_PATH/\${univ}/admin"
    API_ENDPOINT_TABLE="finances/cdv/v1/$AMUE_API_ENV_PATH/\${univ}/table"
fi

# Création du fichier de variables (sans secrets)
cat > "config/airflow_variables.json" << EOFVARS
{
  "amue_import_schedule": "0 2 * * *",
  "amue_sync_schedule": "0 6 * * *",
  "amue_monitor_schedule": "0 22 * * *",
  "ecc_import_schedule": "0 4 * * *",
  "universite": "$UNIVERSITE",
  "api_endpoint_admin": "$API_ENDPOINT_ADMIN",
  "api_endpoint_table": "$API_ENDPOINT_TABLE",
  "amue_import_batch_size": "5000",
  "amue_import_parallel_workers": "1",
  "amue_api_max_retries": "3",
  "amue_api_retry_delay_seconds": "30",
  "amue_force_import": "false",
  "amue_polling_interval_minutes": "10",
  "amue_max_wait_hours": "6",
  "amue_polling_exponential_backoff": "false",
  "amue_polling_max_backoff_minutes": "60",
  "amue_pre_import_dags": "[]",
  "amue_post_import_dags": "[]",
  "ecc_import_batch_size": "5000",
  "amue_reports_dir": "/opt/airflow/logs/reports",
  "amue_report_recipients": "",
  "ecc_report_recipients": "",
  "smtp_host": "$SMTP_HOST",
  "smtp_port": "$SMTP_PORT",
  "smtp_use_tls": "false",
  "smtp_timeout": "30",
  "smtp_mail_from": "$SMTP_MAIL_FROM",
  "smtp_sender_name": "Airflow",
  "TYPE_MAPPING_SQLITE_TO_POSTGRES": {
    "TEXT": "TEXT",
    "CLOB": "TEXT",
    "VARCHAR": "VARCHAR",
    "NVARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "CHARACTER": "CHAR",
    "NCHAR": "CHAR",
    "INTEGER": "INTEGER",
    "TINYINT": "SMALLINT",
    "SMALLINT": "SMALLINT",
    "MEDIUMINT": "INTEGER",
    "BIGINT": "BIGINT",
    "INT2": "SMALLINT",
    "INT8": "BIGINT",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "DEC": "NUMERIC",
    "BOOLEAN": "BOOLEAN",
    "REAL": "DOUBLE PRECISION",
    "DOUBLE": "DOUBLE PRECISION",
    "FLOAT": "DOUBLE PRECISION",
    "DATE": "TIMESTAMP",
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

log_info "Arrêt des containers existants et suppression des volumes..."
$DOCKER_CMD down -v 2>/dev/null || true

log_info "Construction de l'image Docker (installe les dépendances de requirements.txt)..."
if ! $DOCKER_CMD build; then
    log_error "Échec de la construction de l'image Docker"
    exit 1
fi
log_success "Image construite"

log_info "Démarrage des containers (cela peut prendre quelques minutes)..."
$DOCKER_CMD up -d

# Attend que airflow-apiserver soit en etat 'Up' (pas juste 'Started')
# Le container peut mettre quelques secondes a stabiliser apres docker-compose up -d
log_info "Attente du demarrage de airflow-apiserver..."
_up_retries=15
_up_sleep=2
while [[ $_up_retries -gt 0 ]]; do
    if $DOCKER_CMD ps | grep "airflow-apiserver" | grep -q "Up"; then
        break
    fi
    ((_up_retries--))
    if [[ $_up_retries -eq 0 ]]; then
        log_error "Le service airflow-apiserver n'a pas demarre correctement"
        log_info "Etat des containers:"
        $DOCKER_CMD ps
        echo ""
        log_info "Logs airflow-apiserver (50 dernieres lignes):"
        $DOCKER_CMD logs airflow-apiserver --tail=50 2>&1 || true
        exit 1
    fi
    sleep "$_up_sleep"
done

log_success "Containers demarres"

###############################################################################
# Étape 7: Configuration d'Airflow
###############################################################################

log_info "Étape 7/9: Configuration d'Airflow"

# Attendre que l'API soit disponible
log_info "Attente de l'API Airflow..."
_api_retries=36
while [[ $_api_retries -gt 0 ]]; do
    if curl -s http://localhost:8080/api/v2/version > /dev/null 2>&1; then
        log_success "API Airflow disponible"
        break
    fi
    # Utiliser $(()) au lieu de (()) pour eviter le code de retour 1 quand la
    # valeur atteint 0 avec set -e (bug classique bash)
    _api_retries=$(( _api_retries - 1 ))
    if [[ $_api_retries -eq 0 ]]; then
        log_error "Timeout: API Airflow non disponible apres $(( 36 * 5 ))s"
        log_info "Etat des containers:"
        $DOCKER_CMD ps
        echo ""
        log_info "Logs airflow-apiserver:"
        $DOCKER_CMD logs airflow-apiserver --tail=80 2>&1 || true
        exit 1
    fi
    sleep 5
done

# Lancer le script de configuration (variables non sensibles depuis JSON)
log_info "Configuration des variables Airflow (non sensibles)..."
chmod +x "$SCRIPT_DIR/setup_airflow_config.sh"
"$SCRIPT_DIR/setup_airflow_config.sh" --variables-only

# Injection des variables sensibles directement via CLI (jamais dans le JSON committé)
log_info "Injection des variables sensibles dans Airflow DB..."
airflow_exec variables set smtp_mail_from              "$SMTP_MAIL_FROM"
airflow_exec variables set amue_report_recipients     "$AMUE_REPORT_RECIPIENTS"
airflow_exec variables set ecc_report_recipients      "$AMUE_REPORT_RECIPIENTS"
log_success "Variables sensibles injectées dans Airflow DB (non committées)"

# Injection directe des credentials dans Airflow (jamais sur disque)
log_info "Injection des credentials dans les connexions Airflow..."

$DOCKER_CMD exec -T airflow-apiserver airflow connections delete oauth_api 2>/dev/null || true
if ! airflow_exec connections add oauth_api \
    --conn-type http \
    --conn-host "$AMUE_API_HOST" \
    --conn-login "$OAUTH_CLIENT_ID" \
    --conn-password "$OAUTH_CLIENT_SECRET" \
    --conn-extra "{\"token_url\": \"$AMUE_TOKEN_URL\", \"api_base_url\": \"$AMUE_API_HOST\"}"; then
    log_error "Échec création connexion oauth_api"
fi

$DOCKER_CMD exec -T airflow-apiserver airflow connections delete postgres_data 2>/dev/null || true
if ! airflow_exec connections add postgres_data \
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
    if ! airflow_exec connections add oracle_data \
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
# Attente que postgres-data soit initialisé (init_db.sql doit avoir tourné)
###############################################################################

log_info "Attente que postgres-data soit initialisé..."
_pg_retries=30
while [[ $_pg_retries -gt 0 ]]; do
    if $DOCKER_CMD exec -T postgres-data psql -U "$PG_LOGIN" -d "$PG_DATABASE" \
        -c "SELECT 1 FROM splus_admin.amue_tables LIMIT 1" > /dev/null 2>&1; then
        log_success "postgres-data prêt"
        break
    fi
    log_warning "postgres-data pas encore prêt... ($_pg_retries restants)"
    sleep 3
    _pg_retries=$(( _pg_retries - 1 ))
done
if [[ $_pg_retries -eq 0 ]]; then
    log_error "postgres-data non accessible après $(( 30 * 3 ))s — init_db.sql a-t-il échoué ?"
    exit 1
fi

###############################################################################
# Étape 8/9: Configuration des tables à importer
###############################################################################

log_info "Étape 8/9: Configuration des tables à importer"

echo ""
log_info "Saisissez les tables AMUE à importer (table_name, primary_key, delta)."
log_info "Vous pourrez en ajouter/modifier plus tard via : ./manage.sh load-tables"
echo ""

# Résolution de l'endpoint admin (substitue ${univ} par la valeur réelle)
ADMIN_ENDPOINT_RESOLVED="${API_ENDPOINT_ADMIN/\$\{univ\}/$UNIVERSITE}"

# Tentative d'obtention d'un token OAuth (silencieuse, non bloquante)
ACCESS_TOKEN=""
if [[ -n "$OAUTH_CLIENT_ID" && -n "$OAUTH_CLIENT_SECRET" ]]; then
    _TOKEN_RESP=$(curl -s --max-time 10 -X POST "$AMUE_TOKEN_URL" \
        -u "$OAUTH_CLIENT_ID:$OAUTH_CLIENT_SECRET" \
        -d "grant_type=client_credentials" 2>/dev/null)
    ACCESS_TOKEN=$(echo "$_TOKEN_RESP" | jq -r '.access_token // empty' 2>/dev/null)
    unset _TOKEN_RESP
fi
[[ -n "$ACCESS_TOKEN" ]] \
    && log_info "Token OAuth obtenu — les clés primaires seront récupérées automatiquement depuis l'API" \
    || log_warning "Token OAuth non disponible — saisie manuelle des clés primaires"

TABLE_COUNT=0
while true; do
    echo -n "Ajouter une table ? (o/N) : " >&2
    read -r ADD_TABLE </dev/tty
    # Réponse invalide (ni o/y ni n/vide) : probablement un nom de table tapé au mauvais endroit
    if [[ -n "$ADD_TABLE" && ! "$ADD_TABLE" =~ ^[oOnNyY]$ ]]; then
        log_warning "Répondre 'o' pour ajouter une table, 'n' ou Entrée pour terminer."
        continue
    fi
    if [[ ! "$ADD_TABLE" =~ ^[oOyY]$ ]]; then
        break
    fi

    echo -n "  Nom de la table : " >&2
    read -r T_NAME </dev/tty
    [[ -z "$T_NAME" ]] && continue

    # Auto-fetch de la clé primaire depuis l'API, avec fallback saisie manuelle
    T_PK=""
    if [[ -n "$ACCESS_TOKEN" ]]; then
        _KEYS_RESP=$(curl -s --max-time 10 \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            "$AMUE_API_HOST/$ADMIN_ENDPOINT_RESOLVED?get=$T_NAME.keys&f=json" 2>/dev/null)
        T_PK=$(echo "$_KEYS_RESP" | python3 -c "
import json, sys
text = sys.stdin.read()
try:
    data = json.loads(text)
    if isinstance(data, list):
        print(','.join(str(k) for k in data if k))
    elif isinstance(data, dict):
        keys = data.get('keys', [])
        print(','.join(str(k) for k in keys if k))
    elif isinstance(data, str):
        print(data.strip())
except (json.JSONDecodeError, ValueError):
    cleaned = text.strip()
    if cleaned and not cleaned.startswith('<'):
        print(cleaned)
" 2>/dev/null || true)
        unset _KEYS_RESP
    fi
    if [[ -n "$T_PK" ]]; then
        log_info "  Clé primaire récupérée depuis l'API : $T_PK"
        echo -n "  Clé primaire [$T_PK] : " >&2
        read -r _T_PK_INPUT </dev/tty
        [[ -n "$_T_PK_INPUT" ]] && T_PK="$_T_PK_INPUT"
        unset _T_PK_INPUT
    else
        echo -n "  Clé primaire (séparée par virgules, ex: MANDT,BUKRS,BELNR) : " >&2
        read -r T_PK </dev/tty
    fi

    echo -n "  Colonne delta (laisser vide si import complet) : " >&2
    read -r T_DELTA </dev/tty

    # Interpolation shell directe avec echappement SQL (apostrophes → '')
    # La substitution de variables psql (:'var') ne fonctionne pas via docker exec -T
    _sql_name="${T_NAME//\'/\'\'}"
    _sql_pk="${T_PK//\'/\'\'}"
    _sql_delta="${T_DELTA//\'/\'\'}"
    $DOCKER_CMD exec -T postgres-data psql -U "$PG_LOGIN" -d "$PG_DATABASE" -q \
        -c "INSERT INTO splus_admin.amue_tables (table_name, primary_key, delta)
            VALUES ('$_sql_name', '$_sql_pk', '$_sql_delta')
            ON CONFLICT (table_name) DO UPDATE
              SET primary_key = EXCLUDED.primary_key,
                  delta       = EXCLUDED.delta,
                  updated_at  = NOW();" \
    && log_success "  Table '$T_NAME' enregistree" \
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
    if [[ -z "$USER_PASSWORD" ]]; then
        log_warning "Mot de passe vide, utilisateur ignoré"
        continue
    fi
    if ! validate_password_strength "$USER_PASSWORD" "Mot de passe de $USER_NAME"; then
        log_warning "Utilisateur $USER_NAME ignoré (mot de passe non conforme)"
        continue
    fi

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
airflow_exec users list

###############################################################################
# Vérification finale
###############################################################################

log_info "Vérification finale..."

echo ""
log_info "Vérification des variables..."
airflow_exec variables list | head -20

echo ""
log_info "Vérification des connexions..."
airflow_exec connections list

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
    echo -e "   Endpoints: finances/cdv/v1/..."
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
