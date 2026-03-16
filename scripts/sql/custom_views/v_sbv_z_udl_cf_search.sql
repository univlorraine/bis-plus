CREATE VIEW splus.v_sbv_z_udl_cf_search AS
    SELECT fikrs,
        fictr,
        beschr,
        datbis,
        datab
    FROM {target_schema}.fmfctr t1
    WHERE (
        (fikrs = '1010'::bpchar)
        AND ((fictr >= '0000000000'::bpchar) AND (fictr <= 'ZZZZZZZZZZ'::bpchar))
        AND ((datab IS NULL) OR (datab <= '20250304'::bpchar))
        AND ((datbis IS NULL) OR (datbis >= '20250304'::bpchar)))
    ORDER BY fictr
    LIMIT 50;