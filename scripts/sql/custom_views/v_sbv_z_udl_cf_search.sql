DROP VIEW IF EXISTS splus.v_sbv_z_udl_cf_search;
CREATE VIEW splus.v_sbv_z_udl_cf_search AS
    SELECT fikrs,
           fictr,
           beschr,
           datbis,
           datab
    FROM {target_schema}.fmfctr t1
    WHERE fikrs = '1010'::bpchar
    ORDER BY fictr;

COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.fikrs IS 'Périmètre financier';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.fictr IS 'Centre financier';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.beschr IS 'Description';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.datbis IS 'FM : date val. jusq.';
COMMENT ON COLUMN splus.v_sbv_z_udl_cf_search.datab IS 'FM : date début de validité';