-- View: splus.v_bs_z_udl_read_cp

-- DROP VIEW splus.v_bs_z_udl_read_cp;

CREATE OR REPLACE VIEW splus.v_bs_z_udl_read_cp
 AS
 SELECT prctr,
    kokrs,
    ktext,
    ltext,
    verak,
    ersda,
    datab,
    datbi,
    abtei
   FROM {target_schema}.cepc;

ALTER TABLE splus.v_bs_z_udl_read_cp
    OWNER TO root;

GRANT ALL ON TABLE splus.v_bs_z_udl_read_cp TO root;
GRANT ALL ON TABLE splus.v_bs_z_udl_read_cp TO sifacplus;

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.prctr
    IS 'Centre de profit';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.ktext
    IS 'Désignation générale';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.ltext
    IS 'Texte descriptif';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.verak
    IS 'Centre financier';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.ersda
    IS 'Date de saisie';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.datab
    IS 'Date de début de validité';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.datbi
    IS 'Fin de validité';

COMMENT ON COLUMN splus.v_bs_z_udl_read_cp.abtei
    IS 'Domaine fonctionnel';
