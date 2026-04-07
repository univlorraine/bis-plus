-- View: splus.v_bs_z_udl_fm_hivarnt_read_hie

-- DROP VIEW splus.v_bs_z_udl_fm_hivarnt_read_hie;

CREATE OR REPLACE VIEW splus.v_bs_z_udl_fm_hivarnt_read_hie
 AS
 SELECT t1.fikrs,
    t1.hivarnt,
    t1.fistl,
    t2.erfdat,
    t2.datbis,
    t2.beschr,
    t1.hiroot_st,
    t1.parent_st,
    t1.next_st,
    t1.child_st,
    t1.hilevel
   FROM {target_schema}.fmhisv t1
     LEFT JOIN {target_schema}.fmfctr t2 ON t1.fistl = t2.fictr AND t1.fikrs = t2.fikrs AND t2.datab <= '20250314'::bpchar AND t2.datbis >= '20250314'::bpchar
  WHERE t1.hivarnt = '025'::bpchar;

ALTER TABLE splus.v_bs_z_udl_fm_hivarnt_read_hie
    OWNER TO sifacplus;

GRANT SELECT ON TABLE splus.v_bs_z_udl_fm_hivarnt_read_hie TO anon;
GRANT ALL ON TABLE splus.v_bs_z_udl_fm_hivarnt_read_hie TO sifacplus;

COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.fikrs IS 'Périmètre financier';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.hivarnt IS 'Variante hiérarchie ctre financier';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.fistl IS 'Centre financier';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.hiroot_st IS 'Centre financier le plus élevé dans le sous-arbre';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.parent_st IS 'Centre financier supérieur';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.next_st IS 'Centre financier suivant au même niveau hiérarchique';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.child_st IS 'Centre financier subordonné';
COMMENT ON COLUMN splus.v_bs_z_udl_fm_hivarnt_read_hie.hilevel IS 'FIFM : niveau au sein d\'une hiérarchie';