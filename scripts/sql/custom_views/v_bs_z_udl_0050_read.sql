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