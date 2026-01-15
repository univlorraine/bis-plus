-- Script d'initialisation de la base de données PostgreSQL
-- Crée le schéma splus et les permissions nécessaires

-- Connexion à la base business_data
\c business_data

-- Création du schéma splus
CREATE SCHEMA IF NOT EXISTS splus;

-- Attribution des droits au user datauser
GRANT ALL PRIVILEGES ON SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus TO datauser;

-- Définir le search_path par défaut
ALTER ROLE datauser SET search_path TO splus, public;

-- Log de confirmation
SELECT 'Database initialized successfully' AS status;
SELECT 'Schema splus created' AS info;
SELECT 'Permissions granted to datauser' AS info;