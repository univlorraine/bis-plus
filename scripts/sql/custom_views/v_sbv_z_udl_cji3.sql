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