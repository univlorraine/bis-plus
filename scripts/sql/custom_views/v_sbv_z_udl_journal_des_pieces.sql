DROP VIEW IF EXISTS splus.v_sbv_z_udl_journal_des_pieces;
CREATE VIEW splus.v_sbv_z_udl_journal_des_pieces AS
    SELECT t1.gjahr AS gjahr1,
        t1.perio,
        t2.refbn,
        t1.knbuzei,
        'Echéance'::text AS echeance,
        'Traité'::text AS traite,
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
                {target_schema}.fmifiit t1 LEFT JOIN {target_schema}.ecc_fmifihd t2 ON ((t1.fmbelnr = t2.fmbelnr) AND (t1.fikrs = t2.fikrs))
            ) LEFT JOIN {target_schema}.prps t3 ON ((t3.objnr)::text = concat('PR', substr((t1.objnrz)::text, 3, 8)))
        ) LEFT JOIN {target_schema}.bkpf t4 ON ((t4.belnr = t2.refbn) AND (t4.bukrs = t2.refbk) AND (t4.gjahr = t2.refgj))
    )
    WHERE
    (
        ((t1.gjahr >= '2015'::bpchar) AND (t1.gjahr <= '2020'::bpchar))
        AND (t1.fipex ~~ 'R%'::text)
        AND (t1.measure = 'RQGPGEAS'::bpchar)
        AND (t1.ztraite = (1)::numeric)
    );

COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.perio IS 'Période';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.refbn IS 'Numéro de pièce de la pièce de référence';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.knbuzei IS 'Poste pour n° pièce FI';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.echeance IS 'Echéance';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.traite IS 'Traité';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.vrefbn IS 'N° document précédent';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.posid IS 'Elément d\'organigramme technique de projet (élt OTP)';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.fistl IS 'Centre financier';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.measure IS 'Programme de financement';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.fipex IS 'Compte budgétaire';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.hkont IS 'Compte général de la comptabilité générale';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.farea IS 'Domaine fonctionnel';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.fonds IS 'Fonds';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.trbtr IS 'Montant en devise transaction';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.conso IS 'Conso';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.payflg IS 'Statut de paiement de pièces de Comptabilité budgétaire';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.btart IS 'Type de montant';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.cpudt IS 'Date de saisie de la pièce comptable';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.monat IS 'Mois d\'exercice';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.blart IS 'Type de pièce';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.gjahr2 IS 'Exercice comptable';
COMMENT ON COLUMN splus.v_sbv_z_udl_journal_des_pieces.prctr IS 'Centre de profit';