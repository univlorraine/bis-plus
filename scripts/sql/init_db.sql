-- Script d'initialisation de la base de données PostgreSQL
-- Crée le schéma splus et les schémas Blue/Green avec les permissions nécessaires

-- Connexion à la base business_data


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
-- PERMISSIONS UTILISATEUR COURANT
-- Utilise current_user pour fonctionner quel que soit le login configuré
-- via POSTGRES_USER dans docker-compose / déploiement manuel.
-- ============================================================================

-- Permissions sur le schéma principal (vues)
GRANT ALL PRIVILEGES ON SCHEMA splus TO current_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus TO current_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus TO current_user;

-- Permissions sur splus_blue
GRANT ALL PRIVILEGES ON SCHEMA splus_blue TO current_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_blue TO current_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_blue TO current_user;

-- Permissions sur splus_green
GRANT ALL PRIVILEGES ON SCHEMA splus_green TO current_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_green TO current_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_green TO current_user;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA splus
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO current_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_blue
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO current_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_green
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO current_user;

-- Définir le search_path par défaut (inclut les schémas blue/green)
DO $$
BEGIN
    EXECUTE format('ALTER ROLE %I SET search_path TO splus, splus_blue, splus_green, public', current_user);
END
$$;

-- ============================================================================
-- SCHÉMA ADMIN (état centralisé - remplace variables Airflow d'état)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS splus_admin;

-- ============================================================================
-- TABLE D'ÉTAT CENTRALISÉE
-- Remplace les variables Airflow :
--   - amue_last_finish_timestamp
--   - amue_last_successful_run
--   - amue_bluegreen_state (tous les champs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS splus_admin.amue_state (
    id                    INTEGER PRIMARY KEY DEFAULT 1,

    -- Timestamps de synchro AMUE (polling)
    last_finish_timestamp TIMESTAMPTZ,
    last_successful_run   TIMESTAMPTZ,
    last_report_start     TIMESTAMPTZ,

    -- État blue/green
    active_schema         VARCHAR(20),
    last_switch_timestamp TIMESTAMPTZ,
    last_sync_timestamp   TIMESTAMPTZ,
    import_in_progress    BOOLEAN NOT NULL DEFAULT FALSE,
    import_started_at     TIMESTAMPTZ,
    import_correlation_id VARCHAR(255),

    -- Audit
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT single_row CHECK (id = 1)
);

-- Ligne unique (si elle n'existe pas encore)
INSERT INTO splus_admin.amue_state (id)
VALUES (1)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- TABLE DE CONFIGURATION DES TABLES À IMPORTER
-- Remplace la variable Airflow 'amue_tables_to_import'
-- ============================================================================
CREATE TABLE IF NOT EXISTS splus_admin.amue_tables (
    table_name      VARCHAR(100) PRIMARY KEY,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    primary_key     TEXT         NOT NULL DEFAULT '',
    delta           TEXT         NOT NULL DEFAULT '',
    fingerprint_api TEXT         NOT NULL DEFAULT '',
    fingerprint_local TEXT        NOT NULL DEFAULT '',
    setup_status    VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending / ready / blocked
    ecc_query       TEXT,          -- NULL = table AMUE pure ; non-NULL = requête Oracle ECC
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- -- Config initiale (ON CONFLICT DO NOTHING = idempotent au redémarrage)
-- INSERT INTO splus_admin.amue_tables (table_name, enabled, primary_key, delta) VALUES
--     ('AGR_AGRS',  true,  'AGR_NAME,CHILD_AGR',                                             ''),
--     ('AGR_USERS', true,  'AGR_NAME,UNAME,FROM_DAT,TO_DAT',                                 ''),
--     ('BKPF',      true,  'BUKRS,BELNR,GJAHR',                                              'cpudt'),
--     ('CEPC',      true,  'PRCTR,DATBI,KOKRS',                                              ''),
--     ('COBK',      true,  'KOKRS,BELNR',                                                    ''),
--     ('COVP',      true,  'KOKRS,BELNR,BUZEI',                                              ''),
--     ('CSKS',      true,  'KOKRS,KOSTL,DATBI',                                              ''),
--     ('CSKU',      false, 'SPRAS,KTOPL,KSTAR',                                              ''),
--     ('FM01H',     true,  'FIKRS,GJAHR',                                                    ''),
--     ('FMBDT',     true,  'RLDNR,RRCTY,RVERS,RYEAR,ROBJNR,COBJNR,SOBJNR,RTCUR,DRCRK,RPMAX', ''),
--     ('FMBH',      true,  'DOCNR,FM_AREA,DOCYEAR',                                          'crtdate'),
--     ('FMBL',      true,  'FM_AREA,DOCYEAR,DOCNR,DOCLN,RPMAX',                              ''),
--     ('FMFCTR',    true,  'FIKRS,FICTR,DATBIS',                                             ''),
--     ('FMIFHD',    false, 'FMBELNR,FIKRS',                                                  ''),
--     ('FMIFIIT',   true,  'FIKRS,BTART,RLDNR,GJAHR,STUNR',                                  ''),
--     ('FMFINCODE', true,  'FIKRS,FINCODE',                                                  ''),
--     ('FMHISV',    true,  'FIKRS,HIVARNT,FISTL',                                            ''),
--     ('FMMEASURE', true,  'FMAREA,MEASURE',                                                 ''),
--     ('KBLK',      true,  'BELNR',                                                          ''),
--     ('KNA1',      true,  'KUNNR',                                                          ''),
--     ('LFA1',      true,  'LIFNR',                                                          ''),
--     ('LFB1',      true,  'LIFNR,BUKRS',                                                    ''),
--     ('PA0000',    false, 'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
--     ('PA0001',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
--     ('PA0002',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
--     ('PA0105',    true,  'PERNR,SUBTY,OBJPS,SPRPS,ENDDA,BEGDA,SEQNR',                      ''),
--     ('PRPS',      false, 'POSID',                                                          ''),
--     ('PRPS_RH',   false, 'POSID',                                                          ''),
--     ('SOOD',      false, '',                                                               ''),
--     ('SRGBTBREL', true,  'BRELGUID',                                                       ''),
--     ('TFKB',      true,  'FKBER',                                                          ''),
--     ('TVARVC',    false, 'NAME,TYPE,NUMB',                                                 ''),
--     ('USR02',     true,  'BNAME',                                                          ''),
--     ('UST04',     false, 'BNAME,PROFILE',                                                  ''),
--     ('UST10S',    false, 'PROFN,AKTPS,OBJCT,AUTH',                                         ''),
--     ('UST12',     true,  'OBJCT,AUTH,AKTPS,FIELD,VON,BIS',                                 ''),
--     ('ZSIFACTAFM',false, 'FKBER',                                                          '')
-- ON CONFLICT (table_name) DO NOTHING;

-- ============================================================================
-- SUIVI DES MIGRATIONS APPLICATIVES (scripts/sql/migrations/)
-- Alimentée par `./manage.sh db-migrate` (appelée par `./manage.sh update`)
-- ============================================================================
CREATE TABLE IF NOT EXISTS splus_admin.schema_migrations (
    version     VARCHAR(10)  PRIMARY KEY,   -- ex. '0001'
    description TEXT         NOT NULL DEFAULT '',
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by  VARCHAR(100) NOT NULL DEFAULT current_user
);

-- ============================================================================
-- PERMISSIONS UTILISATEUR COURANT SUR SPLUS_ADMIN
-- ============================================================================
GRANT ALL PRIVILEGES ON SCHEMA splus_admin TO current_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA splus_admin TO current_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA splus_admin TO current_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA splus_admin
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO current_user;

-- ============================================================================
-- LOG DE CONFIRMATION
-- ============================================================================
SELECT 'Database initialized successfully' AS status;
SELECT 'Schema splus created (views)' AS info;
SELECT 'Schema splus_blue created (blue tables)' AS info;
SELECT 'Schema splus_green created (green tables)' AS info;
SELECT 'Blue/Green architecture ready' AS info;
SELECT 'Permissions granted to ' || current_user AS info;
SELECT 'Schema splus_admin created' AS info;
SELECT 'Table splus_admin.amue_state ready' AS info;
SELECT 'Table splus_admin.amue_tables ready' AS info;
SELECT 'Table splus_admin.schema_migrations ready' AS info;
