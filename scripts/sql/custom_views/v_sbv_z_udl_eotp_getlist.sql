DROP VIEW IF EXISTS splus.v_sbv_z_udl_eotp_getlist;
CREATE VIEW splus.v_sbv_z_udl_eotp_getlist AS
    SELECT posid,
        post1,
        prctr
    FROM {target_schema}.prps
    WHERE (post1 ~~ '%PROTECTION%'::text)
    ORDER BY posid;