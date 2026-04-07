DROP VIEW IF EXISTS splus.v_sbv_z_udl_read_eotp_edi;
CREATE VIEW splus.v_sbv_z_udl_read_eotp_edi AS
    SELECT posid,
        pstrt,
        pende,
        fkstl
    FROM {target_schema}.prps t1
    WHERE (pkokr = '1010'::bpchar);

COMMENT ON COLUMN splus.v_sbv_z_udl_read_eotp_edi.posid IS 'Elément d''organigramme technique de projet (élt OTP)';
COMMENT ON COLUMN splus.v_sbv_z_udl_read_eotp_edi.pstrt IS 'Date de début de base de l''élément d''OTP';
COMMENT ON COLUMN splus.v_sbv_z_udl_read_eotp_edi.pende IS 'Date de fin de base de l''élément d''OTP';
COMMENT ON COLUMN splus.v_sbv_z_udl_read_eotp_edi.fkstl IS 'Centre de coûts associé';