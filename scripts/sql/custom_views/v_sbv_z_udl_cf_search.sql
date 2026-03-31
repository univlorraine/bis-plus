DROP VIEW IF EXISTS splus.v_sbv_z_udl_cf_search;
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

COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.fikrs IS 'Périmètre financier';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.fictr IS 'Centre financier';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.beschr IS 'Description';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.datbis IS 'FM : date val. jusq.';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.datab IS 'FM : date début de validité';