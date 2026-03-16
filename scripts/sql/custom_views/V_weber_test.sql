CREATE VIEW splus.V_weber_test AS
    SELECT t1.gjahr AS gjahr1,
        t1.perio,
        t2.refbn,
        t1.knbuzei,
        'EchÃ©ance'::text AS echeance,
        'TraitÃ©'::text AS traite,
        t1.vrefbn,
        t3.posid,
        t1.fistl,
        t1.measure,
        t1.fipex,
        t1.hkont,
        t1.farea,
        t1.fonds,
        t1.trbtr,
        'conso'::text AS conso,
        t1.payflg,
        t1.btart,
        t4.cpudt,
        t4.monat,
        t4.blart,
        t4.gjahr AS gjahr2,
        t1.prctr
    FROM
    (
        (
            (
                {target_schema}.fmifiit t1 LEFT JOIN {target_schema}.ecc_fmifihd t2
                    ON ((t1.fmbelnr = t2.fmbelnr) AND (t1.fikrs = t2.fikrs))
            ) LEFT JOIN {target_schema}.prps t3
                ON ((t3.objnr)::text = concat('PR', substr((t1.objnrz)::text, 3, 8)))
        ) LEFT JOIN {target_schema}.bkpf t4
            ON ((t4.belnr = t2.refbn) AND (t4.bukrs = t2.refbk) AND (t4.gjahr = t2.refgj))
    )
    WHERE (
        (t1.gjahr >= '2015'::bpchar)
        AND (t1.gjahr <= '2020'::bpchar)
        AND (t1.fipex ~~ 'R%'::text)
        AND (t1.measure = 'RQGPGEAS'::bpchar)
        AND (t1.ztraite = (1)::numeric)
    );