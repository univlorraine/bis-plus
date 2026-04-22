DROP VIEW IF EXISTS splus.v_bs_udl_check_auth_bs;
CREATE VIEW splus.v_bs_udl_check_auth_bs
AS
    SELECT uname,
        CASE agr_name
            WHEN 'YC_BUDGETS_CONTROLEUR_N2'::bpchar THEN '5'::text
            WHEN 'YC_BUDGETS_ADMINISTRATEUR'::bpchar THEN '4'::text
            WHEN 'YC_BUDGETS_CONTROLEUR'::bpchar THEN '3'::text
            WHEN 'YC_BUDGETS_GESTIONNAIRE'::bpchar THEN '2'::text
            WHEN 'YC_BUDGETS_CONSULTATION'::bpchar THEN '1'::text
            ELSE NULL::text
        END AS activite,
        from_dat,
        to_dat
    FROM ( SELECT s.agr_name,
                  s.text,
                  s.spras,
                  s.uname,
                  s.from_dat,
                  s.to_dat,
                  s.exclude,
                  s.change_dat,
                  s.change_tim,
                  s.change_tst,
                  s.org_flag,
                  s.col_flag,
                  row_number() OVER (PARTITION BY s.uname ORDER BY (
                      CASE s.agr_name
                          WHEN 'YC_BUDGETS_CONTROLEUR_N2'::bpchar THEN '5'::text
                          WHEN 'YC_BUDGETS_ADMINISTRATEUR'::bpchar THEN '4'::text
                          WHEN 'YC_BUDGETS_CONTROLEUR'::bpchar THEN '3'::text
                          WHEN 'YC_BUDGETS_GESTIONNAIRE'::bpchar THEN '2'::text
                          WHEN 'YC_BUDGETS_CONSULTATION'::bpchar THEN '1'::text
                          ELSE NULL::text
                      END) DESC) AS rn
           FROM {target_schema}.agr_users s
           WHERE s.agr_name ~~ 'YC_BUDGETS_%'::text) unnamed_subquery
    WHERE rn = 1;