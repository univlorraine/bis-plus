-- Script d'initialisation de la base de données PostgreSQL
-- Crée le schéma splus et les schémas Blue/Green avec les permissions nécessaires

-- Connexion à la base business_data
\c business_data

-- ============================================================================
-- SCHÉMA PRINCIPAL (vues publiques)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS splus;

-- ============================================================================
-- SCHÉMAS BLUE/GREEN (tables de données)
-- ============================================================================
-- Architecture Blue/Green pour imports atomiques avec rollback
-- - splus_blue  : Tables blue
-- - splus_green : Tables green (identiques)
-- - splus       : Vues pointant vers le schéma actif
CREATE SCHEMA IF NOT EXISTS splus_blue;
CREATE SCHEMA IF NOT EXISTS splus_green;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

-- Permissions sur le schéma principal (vues)
GRANT ALL PRIVILEGES ON SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus TO datauser;

-- Permissions sur splus_blue
GRANT ALL PRIVILEGES ON SCHEMA splus_blue TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_blue TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_blue TO datauser;

-- Permissions sur splus_green
GRANT ALL PRIVILEGES ON SCHEMA splus_green TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_green TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_green TO datauser;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA splus
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_blue
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_green
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO datauser;

-- Définir le search_path par défaut (inclut les schémas blue/green)
ALTER ROLE datauser SET search_path TO splus, splus_blue, splus_green, public;

-- ============================================================================
-- SCHÉMA ADMIN (état centralisé - remplace variables Airflow d'état)
-- ============================================================================
\i /docker-entrypoint-initdb.d/init_admin_schema.sql

-- ============================================================================
-- LOG DE CONFIRMATION
-- ============================================================================
SELECT 'Database initialized successfully' AS status;
SELECT 'Schema splus created (views)' AS info;
SELECT 'Schema splus_blue created (blue tables)' AS info;
SELECT 'Schema splus_green created (green tables)' AS info;
SELECT 'Blue/Green architecture ready' AS info;
SELECT 'Permissions granted to datauser' AS info;