DROP VIEW IF EXISTS splus.v_bs_z_udl_read_cc;
CREATE VIEW splus.v_bs_z_udl_read_cc AS
    SELECT kokrs,
        kostl,
        ktext,
        ltext,
        verak,
        ersda,
        datab,
        datbi,
        prctr
    FROM {target_schema}.csks code;

COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.kokrs IS 'Périmètre analytique';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.kostl IS 'Centre de coûts';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.ktext IS 'Désignation générale';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.ltext IS 'Description';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.verak IS 'Centre financier';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.ersda IS 'Date de saisie';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.datab IS 'Date de début de validité';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.datbi IS 'Fin de validité';
COMMENT ON COLUMN splus.v_bs_z_udl_read_cc.prctr IS 'Centre de profit';