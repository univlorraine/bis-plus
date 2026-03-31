DROP VIEW IF EXISTS splus.v_bs_z_udl_read_pfis;
CREATE VIEW splus.v_bs_z_udl_read_pfis AS
    SELECT t1.measure,
        t1.valid_from,
        t1.valid_to,
        t1.short_desc,
        t1.description,
        t1.fp_type,
        COALESCE(t3.rfundsctr, 'NA'::bpchar) AS rfundsctr,
        t1.created_on,
        t1.modified_on,
        t1.zfleche
    FROM ({target_schema}.fmmeasure t1
        LEFT JOIN (
            SELECT t3_1.rldnr,
                t3_1.rrcty,
                t3_1.rvers,
                t3_1.ryear,
                t3_1.robjnr,
                t3_1.cobjnr,
                t3_1.sobjnr,
                t3_1.rtcur,
                t3_1.drcrk,
                t3_1.rpmax,
                t3_1.rfikrs,
                t3_1.rfund,
                t3_1.rfundsctr,
                t3_1.rcmmtitem,
                t3_1.rfuncarea,
                t3_1.ruserdim,
                t3_1.rgrant_nbr,
                t3_1.rmeasure,
                t3_1.budget_pd_9,
                t3_1.ceffyear_9,
                t3_1.valtype_9,
                t3_1.wfstate_9,
                t3_1.process_9,
                t3_1.budtype_9,
                t3_1.logsys,
                t3_1.tslvt,
                t3_1.tsl01,
                t3_1.tsl02,
                t3_1.tsl03,
                t3_1.tsl04,
                t3_1.tsl05,
                t3_1.tsl06,
                t3_1.tsl07,
                t3_1.tsl08,
                t3_1.tsl09,
                t3_1.tsl10,
                t3_1.tsl11,
                t3_1.tsl12,
                t3_1.tsl13,
                t3_1.tsl14,
                t3_1.tsl15,
                t3_1.tsl16,
                t3_1.hslvt,
                t3_1.hsl01,
                t3_1.hsl02,
                t3_1.hsl03,
                t3_1.hsl04,
                t3_1.hsl05,
                t3_1.hsl06,
                t3_1.hsl07,
                t3_1.hsl08,
                t3_1.hsl09,
                t3_1.hsl10,
                t3_1.hsl11,
                t3_1.hsl12,
                t3_1.hsl13,
                t3_1.hsl14,
                t3_1.hsl15,
                t3_1.hsl16,
                t3_1.kslvt,
                t3_1.ksl01,
                t3_1.ksl02,
                t3_1.ksl03,
                t3_1.ksl04,
                t3_1.ksl05,
                t3_1.ksl06,
                t3_1.ksl07,
                t3_1.ksl08,
                t3_1.ksl09,
                t3_1.ksl10,
                t3_1.ksl11,
                t3_1.ksl12,
                t3_1.ksl13,
                t3_1.ksl14,
                t3_1.ksl15,
                t3_1.ksl16,
                t3_1.cspred,
                t3_1.ctem_category_9,
                row_number() OVER (PARTITION BY t3_1.rmeasure ORDER BY t3_1.rmeasure) AS rn
            FROM {target_schema}.fmbdt t3_1
                WHERE (
                    (t3_1.ryear = '2025'::bpchar)
                    AND (t3_1.rldnr = '9F'::bpchar)
                    AND (t3_1.rvers = '000'::bpchar))
                )
        t3 ON ((t1.measure = t3.rmeasure)))
    WHERE (
        (t1.fmarea = '1010'::bpchar)
        AND ((t3.rn = 1) OR (t3.rn IS NULL))
        AND (
                ((t1.modified_on = '00000000'::bpchar) AND (t1.created_on >= '20250303'::bpchar))
                OR (t1.modified_on >= '20250303'::bpchar)
            )
        );

COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.measure IS 'Programme de financement';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.valid_from IS 'FM : date début de validité';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.valid_to IS 'FM : date val. jusq.';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.short_desc IS 'Désignation';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.description IS 'Description';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.fp_type IS 'Type de programme de financement';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.rfundsctr IS 'Centre financier';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.created_on IS 'FIFM : date de saisie';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.modified_on IS 'FIFM : date modification';
COMMENT ON COLUMN splus.v_bs_z_udl_read_pfis.zfleche IS 'Programme de financement fléché';