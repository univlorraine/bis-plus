-- scripts/sql/init_ecc_tables.sql
-- DEPRECATED: Remplacé par splus_admin.amue_tables.ecc_query
-- La table ecc_tables est conservée en base (politique no-delete) mais n'est plus utilisée.
-- Les requêtes ECC sont désormais stockées dans splus_admin.amue_tables (colonne ecc_query).
-- Création de la table de configuration des imports ECC
-- Prérequis : init_admin_schema.sql doit avoir été exécuté (schéma splus_admin)

CREATE TABLE IF NOT EXISTS splus_admin.ecc_tables (
    table_name  VARCHAR(100) PRIMARY KEY,
    sql_file    VARCHAR(500) NOT NULL,
    primary_key TEXT         NOT NULL,   -- colonnes PKs séparées par virgule, minuscules
    enabled     BOOLEAN      NOT NULL DEFAULT TRUE,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO splus_admin.ecc_tables (table_name, sql_file, primary_key, enabled) VALUES
    ('lfa1',   '/opt/airflow/scripts/sql/ECC/SELECT_LFA1.sql',   'lifnr',                                     TRUE),
    ('lfb1',   '/opt/airflow/scripts/sql/ECC/SELECT_LFB1.sql',   'lifnr,bukrs',                               TRUE),
    ('pa0001', '/opt/airflow/scripts/sql/ECC/SELECT_PA0001.sql', 'pernr,subty,objps,sprps,endda,begda,seqnr', TRUE),
    ('pa0002', '/opt/airflow/scripts/sql/ECC/SELECT_PA0002.sql', 'pernr,subty,objps,sprps,endda,begda,seqnr', TRUE)
ON CONFLICT (table_name) DO NOTHING;
