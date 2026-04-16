DROP VIEW IF EXISTS splus.v_sbv_z_udl_eotp_getlist;
CREATE VIEW splus.v_sbv_z_udl_eotp_getlist AS
    SELECT posid,
        post1,
        prctr
    FROM {target_schema}.prps
    ORDER BY posid;

COMMENT ON COLUMN splus.v_sbv_z_udl_eotp_getlist.posid IS 'Elément d''organigramme technique de projet (élt OTP)';
COMMENT ON COLUMN splus.v_sbv_z_udl_eotp_getlist.post1 IS 'PS: désignation (première ligne de texte)';
COMMENT ON COLUMN splus.v_sbv_z_udl_eotp_getlist.prctr IS 'Centre de profit';