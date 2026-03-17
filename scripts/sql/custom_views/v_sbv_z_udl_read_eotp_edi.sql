DROP VIEW IF EXISTS splus.v_sbv_z_udl_read_eotp_edi;
CREATE VIEW splus.v_sbv_z_udl_read_eotp_edi AS
    SELECT posid,
        pstrt,
        pende,
        fkstl
    FROM {target_schema}.prps t1
    WHERE (pkokr = '1010'::bpchar);