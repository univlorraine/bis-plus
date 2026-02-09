-- ============================================================================
-- Script de migration des tables existantes vers l'architecture Blue/Green
-- ============================================================================
--
-- Ce script migre les tables existantes du schéma splus vers blue/green :
-- 1. Crée les tables dans splus_blue et splus_green (copie structure)
-- 2. Copie les données vers les deux schémas
-- 3. Supprime les tables originales dans splus
-- 4. Crée les vues dans splus pointant vers splus_blue (schéma actif initial)
--
-- ATTENTION: Ce script modifie la structure de la base de données!
-- Faites une sauvegarde avant d'exécuter.
--
-- Usage:
--   psql -d your_database -f migrate_to_bluegreen.sql
--
-- ============================================================================

-- Vérification des prérequis
DO $$
BEGIN
    -- Vérifie que les schémas blue/green existent
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'splus_blue') THEN
        RAISE EXCEPTION 'Schéma splus_blue inexistant. Exécutez create_bluegreen_schemas.sql d''abord.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'splus_green') THEN
        RAISE EXCEPTION 'Schéma splus_green inexistant. Exécutez create_bluegreen_schemas.sql d''abord.';
    END IF;

    RAISE NOTICE 'Prérequis vérifiés. Démarrage de la migration...';
END $$;


-- ============================================================================
-- Fonction de migration d'une table
-- ============================================================================
CREATE OR REPLACE FUNCTION migrate_table_to_bluegreen(p_table_name TEXT)
RETURNS VOID AS $$
DECLARE
    v_count_blue INTEGER;
    v_count_green INTEGER;
BEGIN
    RAISE NOTICE 'Migration de la table: %', p_table_name;

    -- 1. Crée la table dans splus_blue (copie structure incluant contraintes)
    EXECUTE format('CREATE TABLE IF NOT EXISTS splus_blue.%I (LIKE splus.%I INCLUDING ALL)', p_table_name, p_table_name);

    -- 2. Crée la table dans splus_green (copie structure)
    EXECUTE format('CREATE TABLE IF NOT EXISTS splus_green.%I (LIKE splus.%I INCLUDING ALL)', p_table_name, p_table_name);

    -- 3. Copie les données vers splus_blue
    EXECUTE format('INSERT INTO splus_blue.%I SELECT * FROM splus.%I ON CONFLICT DO NOTHING', p_table_name, p_table_name);
    EXECUTE format('SELECT COUNT(*) FROM splus_blue.%I', p_table_name) INTO v_count_blue;

    -- 4. Copie les données vers splus_green
    EXECUTE format('INSERT INTO splus_green.%I SELECT * FROM splus.%I ON CONFLICT DO NOTHING', p_table_name, p_table_name);
    EXECUTE format('SELECT COUNT(*) FROM splus_green.%I', p_table_name) INTO v_count_green;

    -- 5. Supprime la table originale
    EXECUTE format('DROP TABLE splus.%I CASCADE', p_table_name);

    -- 6. Crée la vue dans splus pointant vers splus_blue (schéma actif initial)
    EXECUTE format('CREATE VIEW splus.%I AS SELECT * FROM splus_blue.%I', p_table_name, p_table_name);

    RAISE NOTICE '  - Table %: % lignes (blue: %, green: %)', p_table_name, v_count_blue, v_count_blue, v_count_green;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- Migration de toutes les tables
-- ============================================================================
DO $$
DECLARE
    r RECORD;
    v_table_count INTEGER := 0;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Début de la migration Blue/Green';
    RAISE NOTICE '========================================';

    -- Parcourt toutes les tables dans splus (exclut les vues)
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'splus'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    LOOP
        PERFORM migrate_table_to_bluegreen(r.table_name);
        v_table_count := v_table_count + 1;
    END LOOP;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Migration terminée: % tables migrées', v_table_count;
    RAISE NOTICE 'Schéma actif initial: splus_blue';
    RAISE NOTICE '========================================';
END $$;


-- ============================================================================
-- Nettoyage de la fonction temporaire
-- ============================================================================
DROP FUNCTION IF EXISTS migrate_table_to_bluegreen(TEXT);


-- ============================================================================
-- Vérification finale
-- ============================================================================
DO $$
DECLARE
    v_blue_count INTEGER;
    v_green_count INTEGER;
    v_view_count INTEGER;
BEGIN
    -- Compte les tables dans chaque schéma
    SELECT COUNT(*) INTO v_blue_count
    FROM information_schema.tables
    WHERE table_schema = 'splus_blue' AND table_type = 'BASE TABLE';

    SELECT COUNT(*) INTO v_green_count
    FROM information_schema.tables
    WHERE table_schema = 'splus_green' AND table_type = 'BASE TABLE';

    SELECT COUNT(*) INTO v_view_count
    FROM information_schema.views
    WHERE table_schema = 'splus';

    RAISE NOTICE '';
    RAISE NOTICE 'État final:';
    RAISE NOTICE '  - splus_blue : % tables', v_blue_count;
    RAISE NOTICE '  - splus_green: % tables', v_green_count;
    RAISE NOTICE '  - splus (vues): % vues', v_view_count;

    IF v_blue_count != v_green_count THEN
        RAISE WARNING 'ATTENTION: Nombre de tables différent entre blue et green!';
    END IF;
END $$;
