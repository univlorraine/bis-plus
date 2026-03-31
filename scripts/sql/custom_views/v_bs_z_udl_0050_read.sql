DROP VIEW IF EXISTS splus.v_bs_z_udl_0050_read;
CREATE VIEW splus.v_bs_z_udl_0050_read AS
    SELECT t1.docyear,
        t2.docnr,
        t2.docln,
        t2.flg_added,
        t2.fiscyear,
        t2.budcat,
        t2.budtype,
        t2.ctem_category,
        t2.fund,
        t2.fundsctr,
        t2.cmmtitem,
        t2.funcarea,
        t2.measure,
        t2.grant_nbr,
        t2.tcurr,
            CASE t2.ctem_category
                WHEN '3'::bpchar THEN ((((((((((((((((t2.tval01 + t2.tval02) + t2.tval03) + t2.tval04) + t2.tval05) + t2.tval06) + t2.tval07) + t2.tval08) + t2.tval09) + t2.tval10) + t2.tval11) + t2.tval12) + t2.tval13) + t2.tval14) + t2.tval15) + t2.tval16) * ('-1'::integer)::numeric)
                ELSE (((((((((((((((t2.tval01 + t2.tval02) + t2.tval03) + t2.tval04) + t2.tval05) + t2.tval06) + t2.tval07) + t2.tval08) + t2.tval09) + t2.tval10) + t2.tval11) + t2.tval12) + t2.tval13) + t2.tval14) + t2.tval15) + t2.tval16)
            END AS total_amount_tcur,
        t2.text50,
        t2.valtype,
        t2.process,
        t1.crtuser,
        t1.crtdate,
        t1.process_ui,
        t1.revstate,
        t1.rev_refnr,
        t1.doctype
    FROM
    (
        {target_schema}.fmbh t1 LEFT JOIN {target_schema}.fmbl t2
            ON ((t1.fm_area = t2.fm_area) AND (t1.docyear = t2.docyear) AND (t1.docnr = t2.docnr))
    )
    WHERE
    (
        (t1.fm_area = '1010'::bpchar)
        AND (t1.docyear = '2025'::bpchar)
        AND (t1.crtdate >= '20250228'::bpchar)
        AND (t1.process_ui = 'TRAN'::bpchar)
        AND (
            (t1.doctype = 'CVRA'::bpchar)
            OR (t1.doctype = 'REAJ'::bpchar)
            OR (t1.doctype = 'VIEX'::bpchar)
            OR (t1.doctype = 'VIIN'::bpchar)
        )
        AND (
            (t1.docstate = '1'::bpchar)
            OR (t1.docstate = '3'::bpchar)
        )
    ) UNION
        SELECT t1.docyear,
        t2.docnr,
        t2.docln,
        t2.flg_added,
        t2.fiscyear,
        t2.budcat,
        t2.budtype,
        t2.ctem_category,
        t2.fund,
        t2.fundsctr,
        t2.cmmtitem,
        t2.funcarea,
        t2.measure,
        t2.grant_nbr,
        t2.tcurr,
            CASE t2.ctem_category
                WHEN '3'::bpchar THEN ((((((((((((((((t2.tval01 + t2.tval02) + t2.tval03) + t2.tval04) + t2.tval05) + t2.tval06) + t2.tval07) + t2.tval08) + t2.tval09) + t2.tval10) + t2.tval11) + t2.tval12) + t2.tval13) + t2.tval14) + t2.tval15) + t2.tval16) * ('-1'::integer)::numeric)
                ELSE (((((((((((((((t2.tval01 + t2.tval02) + t2.tval03) + t2.tval04) + t2.tval05) + t2.tval06) + t2.tval07) + t2.tval08) + t2.tval09) + t2.tval10) + t2.tval11) + t2.tval12) + t2.tval13) + t2.tval14) + t2.tval15) + t2.tval16)
            END AS total_amount_tcur,
        t2.text50,
        t2.valtype,
        t2.process,
        t1.crtuser,
        t1.crtdate,
        t1.process_ui,
        t1.revstate,
        t1.rev_refnr,
        t1.doctype
        FROM
        (
            ({target_schema}.fmbh t1 JOIN
                (
                    SELECT t3.rev_refnr
                    FROM {target_schema}.fmbh t3
                    WHERE (
                    (t3.fm_area = '1010'::bpchar)
                    AND (t3.docyear = '2025'::bpchar)
                    AND (t3.crtdate >= '20250228'::bpchar)
                    AND (t3.process_ui = 'TRAN'::bpchar)
                    AND ((t3.doctype = 'CVRA'::bpchar) OR (t3.doctype = 'REAJ'::bpchar) OR (t3.doctype = 'VIEX'::bpchar) OR (t3.doctype = 'VIIN'::bpchar))
                    AND ((t3.docstate = '1'::bpchar) OR (t3.docstate = '3'::bpchar))
                    AND (t3.rev_refnr <> ' '::bpchar)
                    )
                ) t4 ON (t1.docnr = t4.rev_refnr)
            ) LEFT JOIN {target_schema}.fmbl t2 ON ((t1.fm_area = t2.fm_area) AND (t1.docyear = t2.docyear) AND (t1.docnr = t2.docnr)))
        WHERE (
            (t1.fm_area = '1010'::bpchar)
                AND (t1.docyear = '2025'::bpchar)
                AND (t1.crtdate >= '20250228'::bpchar)
                AND (t1.process_ui = 'TRAN'::bpchar)
                AND ((t1.doctype = 'CVRA'::bpchar) OR (t1.doctype = 'REAJ'::bpchar) OR (t1.doctype = 'VIEX'::bpchar) OR (t1.doctype = 'VIIN'::bpchar))
                AND ((t1.docstate = '1'::bpchar) OR (t1.docstate = '3'::bpchar)))
    ORDER BY 2, 3;

COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.docyear IS 'Exercice pièce';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.docnr IS 'Numéro de la pièce de saisie du budget';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.docln IS 'Poste de la pièce de saisie de budget';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.flg_added IS 'Code pour ligne supplémentaire';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.fiscyear IS 'Exercice comptable';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.budcat IS 'Catégorie budgétaire';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.budtype IS 'Type de budget';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.ctem_category IS 'Type de compte budgétaire';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.fund IS 'Fonds';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.fundsctr IS 'Centre financier';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.cmmtitem IS 'Compte budgétaire';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.funcarea IS 'Domaine fonctionnel';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.measure IS 'Programme de financement';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.grant_nbr IS 'Subvention';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.tcurr IS 'Devise de transaction';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.total_amount_tcur IS 'Montant total du budget en devise de transaction';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.text50 IS 'Texte du poste';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.valtype IS 'Type de valeur BCS';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.process IS 'Opération de budgétisation interne';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.crtuser IS 'Utilisateur ayant créé ou mis à jour l\'objet';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.crtdate IS 'Date de création  ou de mise à jour de l\'objet';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.process_ui IS 'Opération de budgétisation';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.revstate IS 'Statut de contre-passation';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.rev_refnr IS 'Numéro de référence de la pièce d\'annulation';
 COMMENT ON COLUMN splus.v_bs_z_udl_0050_read.doctype IS 'Type de pièce de saisie du budget';