DROP VIEW IF EXISTS splus.v_sbv_z_udl_check_auth_sbv;
CREATE VIEW splus.v_sbv_z_udl_check_auth_sbv AS
    SELECT DISTINCT uname,
        max(
            CASE agr_name
                WHEN 'YC_SBV_CONSULTATION'::bpchar THEN '7'::text
                WHEN 'YC_SBV_GEST_ORDO'::bpchar THEN '6'::text
                WHEN 'YC_SBV_GEST_DRV_DBF'::bpchar THEN '5'::text
                WHEN 'YC_SBV_GEST_AC'::bpchar THEN '4'::text
                WHEN 'YC_SBV_ADMIN_ORDO'::bpchar THEN '3'::text
                WHEN 'YC_SBV_ADMIN_AC'::bpchar THEN '2'::text
                WHEN 'YC_SBV_ADMIN_DN'::bpchar THEN '1'::text
                ELSE NULL::text
            END) AS activite
    FROM {target_schema}.agr_users
        WHERE (agr_name ~~ 'YC_SBV_%'::text)
        GROUP BY uname;

COMMENT ON COLUMN splus.v_sbv_z_udl_check_auth_sbv.uname IS 'Nom de l''utilisateur ds fiche utilisateur';
COMMENT ON COLUMN splus.v_sbv_z_udl_check_auth_sbv.activite IS 'CONSULTATION = 7, GEST_ORDO = 6, GEST_DRV_DBF = 5,  GEST_AC = 4, ADMIN_ORDO=3, ADMIN_AC = 2 , ADMIN_DN = 1';