DROP VIEW IF EXISTS splus.v_sbv_z_udl_cji3;
CREATE VIEW splus.v_sbv_z_udl_cji3 AS
    SELECT prps.posid,
        covp.gjahr,
        covp.kokrs,
        covp.belnr,
        covp.wkgbtr,
        covp.kstar,
        csku.ltext,
        covp.twaer,
        covp.hrkft,
        covp.owaer,
        cobk.blart,
        cobk.refbk,
        cobk.refbn,
        cobk.bldat
    FROM
    (
        (
            (
                {target_schema}.prps prps LEFT JOIN {target_schema}.covp covp ON (prps.objnr = covp.objnr)
            ) LEFT JOIN {target_schema}.ecc_csku csku ON ((csku.ktopl = 'Z100'::bpchar) AND (csku.kstar = covp.kstar))
        ) LEFT JOIN {target_schema}.cobk cobk ON ((covp.kokrs = cobk.kokrs) AND (covp.belnr = cobk.belnr))
    );

COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.posid IS 'Elément d\'organigramme technique de projet (élt OTP)';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.gjahr IS 'Exercice comptable';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.kokrs IS 'Périmètre analytique';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.belnr IS 'Nº de pièce';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.wkgbtr IS 'Valeur fixe en devise du périmètre analytique';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.kstar IS 'Nature comptable';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.ltext IS 'Description';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.twaer IS 'Devise de transaction';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.owaer IS 'Clé de devise';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.blart IS 'Type de la pièce de référence FI';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.refbk IS 'Société de la pièce FI';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.refbn IS 'Numéro de pièce de la pièce de référence';
COMMENT ON COLUMN splus.v_sbv_z_udl_cji3.bldat IS 'Date de la pièce';