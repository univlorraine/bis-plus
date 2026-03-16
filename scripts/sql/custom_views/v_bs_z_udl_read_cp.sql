CREATE VIEW splus.v_bs_z_udl_read_cp AS
    SELECT prctr,
        ktext,
        ltext,
        verak,
        ersda,
        datab,
        datbi,
        abtei
    FROM {target_schema}.cepc;