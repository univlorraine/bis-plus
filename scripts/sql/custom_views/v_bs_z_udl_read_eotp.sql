DROP VIEW IF EXISTS splus.v_bs_z_udl_read_eotp;
CREATE VIEW splus.v_bs_z_udl_read_eotp
 AS
 SELECT pspnr,
    posid,
    post1,
    pkokr,
    erdat,
    aedat,
    prctr,
    fkstl,
    pstrt,
    pende
   FROM {target_schema}.prps;

ALTER TABLE splus.v_bs_z_udl_read_eotp
    OWNER TO root;

GRANT ALL ON TABLE splus.v_bs_z_udl_read_eotp TO root;
GRANT ALL ON TABLE splus.v_bs_z_udl_read_eotp TO sifacplus;

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.pspnr
    IS 'Elément d''OTP';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.posid
    IS 'Elément d''organigramme technique de projet (élt OTP)';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.post1
    IS 'PS: désignation (première ligne de texte)';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.erdat
    IS 'Date de création de l''enregistrement';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.aedat
    IS 'Date de dernière modification d''un objet';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.prctr
    IS 'Centre de profit';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.fkstl
    IS 'Centre de coÃ»ts associé';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.pstrt
    IS 'Date de début de base de l''élément d''OTP';

COMMENT ON COLUMN splus.v_bs_z_udl_read_eotp.pende
    IS 'Date de fin de base de l''élément d''OTP';
