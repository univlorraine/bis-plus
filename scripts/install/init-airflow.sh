#!/bin/bash
set -e

echo "======================================"
echo "🚀 Initialisation d'Airflow"
echo "======================================"

# Attendre que l'API Airflow soit disponible
echo "⏳ Attente de l'API Airflow..."
until curl -s http://airflow-apiserver:8080/api/v2/health | grep -q "healthy"; do
    echo "   En attente de l'API..."
    sleep 5
done

echo "✓ API Airflow disponible"

# Configuration de l'authentification pour l'API
AUTH="airflow:airflow"
API_URL="http://airflow-apiserver:8080/api/v2"

echo ""
echo "======================================"
echo "📝 Création des Variables Airflow"
echo "======================================"

# Fonction pour créer/mettre à jour une variable
create_variable() {
    local key=$1
    local value=$2
    local description=$3

    echo "→ Variable: $key"

    # Vérifie si la variable existe
    if curl -s -u "$AUTH" "$API_URL/variables/$key" | grep -q "\"key\""; then
        echo "  ⚠️  Variable existe déjà, mise à jour..."
        curl -X PATCH -u "$AUTH" \
            -H "Content-Type: application/json" \
            "$API_URL/variables/$key" \
            -d "{\"key\": \"$key\", \"value\": \"$value\"}" \
            -s -o /dev/null
    else
        echo "  ✓ Création de la variable..."
        curl -X POST -u "$AUTH" \
            -H "Content-Type: application/json" \
            "$API_URL/variables" \
            -d "{\"key\": \"$key\", \"value\": \"$value\", \"description\": \"$description\"}" \
            -s -o /dev/null
    fi
}

# Variables pour le DAG OAuth to PostgreSQL
create_variable "oauth_api_connection_id" "oauth_api" "Connection ID pour l'API OAuth"
create_variable "api_endpoint" "finances/cdv/v1/preprod/ul/table" "Endpoint de l'API"

# Variables pour le DAG AMUE dynamique
create_variable "amue_table_name" "CSKS" "Nom de la table AMUE à importer"

# Variables pour le DAG de failure/recovery
create_variable "failure_dag_auto_retry" "true" "Active/désactive les retries automatiques"
create_variable "failure_dag_max_retries" "3" "Nombre maximum de retries"
create_variable "failure_dag_retry_delay" "60" "Délai entre retries (secondes)"
create_variable "failure_dag_failure_rate" "0.7" "Taux d'échec simulé (0.0 à 1.0)"
create_variable "manual_override_failure" "false" "Override pour forcer le succès"

echo ""
echo "======================================"
echo "🔌 Création des Connections Airflow"
echo "======================================"

# Fonction pour créer/mettre à jour une connection
create_connection() {
    local conn_id=$1
    local conn_type=$2
    local host=$3
    local login=$4
    local password=$5
    local port=$6
    local schema=$7
    local extra=$8
    local description=$9

    echo "→ Connection: $conn_id"

    # Vérifie si la connection existe
    if curl -s -u "$AUTH" "$API_URL/connections/$conn_id" | grep -q "\"connection_id\""; then
        echo "  ⚠️  Connection existe déjà, mise à jour..."
        curl -X PATCH -u "$AUTH" \
            -H "Content-Type: application/json" \
            "$API_URL/connections/$conn_id" \
            -d "{
                \"connection_id\": \"$conn_id\",
                \"conn_type\": \"$conn_type\",
                \"host\": \"$host\",
                \"login\": \"$login\",
                \"password\": \"$password\",
                \"port\": $port,
                \"schema\": \"$schema\",
                \"extra\": \"$extra\"
            }" \
            -s -o /dev/null
    else
        echo "  ✓ Création de la connection..."
        curl -X POST -u "$AUTH" \
            -H "Content-Type: application/json" \
            "$API_URL/connections" \
            -d "{
                \"connection_id\": \"$conn_id\",
                \"conn_type\": \"$conn_type\",
                \"host\": \"$host\",
                \"login\": \"$login\",
                \"password\": \"$password\",
                \"port\": $port,
                \"schema\": \"$schema\",
                \"extra\": \"$extra\",
                \"description\": \"$description\"
            }" \
            -s -o /dev/null
    fi
}

# Connection PostgreSQL pour les données métier
create_connection \
    "postgres_data" \
    "postgres" \
    "postgres-data" \
    "datauser" \
    "datapass" \
    "5432" \
    "business_data" \
    "" \
    "Base de données PostgreSQL pour les données métier"

# Connection OAuth API AMUE
# IMPORTANT: Remplacez YOUR_CLIENT_ID et YOUR_CLIENT_SECRET par vos vraies valeurs
create_connection \
    "oauth_api" \
    "http" \
    "https://sandbox.api.amue.fr" \
    "${AMUE_CLIENT_ID:-YOUR_CLIENT_ID}" \
    "${AMUE_CLIENT_SECRET:-YOUR_CLIENT_SECRET}" \
    "443" \
    "" \
    "{\"token_url\": \"https://sandbox.auth.amue.fr/auth/fer/oauth/token\", \"api_base_url\": \"https://sandbox.api.amue.fr\"}" \
    "API AMUE avec authentification OAuth"

echo ""
echo "======================================"
echo "✅ Initialisation terminée avec succès"
echo "======================================"
echo ""
echo "📊 Résumé:"
echo "  • Variables créées: 7"
echo "  • Connections créées: 2"
echo ""
echo "🔧 Configuration:"
echo "  • Variables: http://localhost:8080/variable/list/"
echo "  • Connections: http://localhost:8080/connection/list/"
echo ""
echo "⚠️  ATTENTION:"
echo "  Pour utiliser l'API AMUE, vous devez définir:"
echo "  • AMUE_CLIENT_ID dans le fichier .env"
echo "  • AMUE_CLIENT_SECRET dans le fichier .env"
echo ""
echo "  Ou modifier manuellement la connection 'oauth_api'"
echo "  dans l'interface web d'Airflow."
echo ""
echo "======================================"