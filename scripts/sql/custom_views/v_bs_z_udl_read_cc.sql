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