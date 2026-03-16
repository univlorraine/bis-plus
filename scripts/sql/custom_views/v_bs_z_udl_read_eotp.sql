CREATE VIEW splus.v_bs_z_udl_read_eotp AS
    SELECT pspnr,
        posid,
        post1,
        erdat,
        aedat,
        prctr,
        fkstl,
        pstrt,
        pende
    FROM {target_schema}.prps;