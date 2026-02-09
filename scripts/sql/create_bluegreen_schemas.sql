-- ============================================================================
-- Script de création des schémas Blue/Green pour l'import AMUE
-- ============================================================================
--
-- Ce script initialise l'architecture Blue/Green avec :
-- - Schéma splus_blue  : Tables blue
-- - Schéma splus_green : Tables green (identiques)
-- - Les vues dans splus continueront de pointer vers le schéma actif
--
-- Usage:
--   psql -d your_database -f create_bluegreen_schemas.sql
--
-- ============================================================================

-- Création des schémas blue et green
CREATE SCHEMA IF NOT EXISTS splus_blue;
CREATE SCHEMA IF NOT EXISTS splus_green;

-- Attribution des permissions au user airflow
-- Adaptez 'airflow' au nom de votre utilisateur PostgreSQL
GRANT ALL ON SCHEMA splus_blue TO airflow;
GRANT ALL ON SCHEMA splus_green TO airflow;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_blue
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_green
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow;

-- Vérification
DO $$
BEGIN
    RAISE NOTICE 'Schémas Blue/Green créés avec succès:';
    RAISE NOTICE '  - splus_blue';
    RAISE NOTICE '  - splus_green';
END $$;
