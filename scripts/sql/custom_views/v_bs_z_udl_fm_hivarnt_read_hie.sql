DROP VIEW IF EXISTS splus.v_bs_z_udl_fm_hivarnt_read_hie;
CREATE VIEW splus.v_bs_z_udl_fm_hivarnt_read_hie AS
    SELECT t1.fikrs,
        t1.hivarnt,
        t1.fistl,
        t1.hiroot_st,
        t1.parent_st,
        t1.next_st,
        t1.child_st,
        t1.hilevel
    FROM
        (
            {target_schema}.fmhisv t1 LEFT JOIN {target_schema}.fmfctr t2
                ON ((t1.fistl = t2.fictr) AND (t1.fikrs = t2.fikrs) AND (t2.datab <= '20250314'::bpchar) AND (t2.datbis >= '20250314'::bpchar))
        )
    WHERE (t1.hivarnt = '025'::bpchar);