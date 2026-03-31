DROP VIEW IF EXISTS splus.v_z_udl_get_auth_values_cf;
CREATE VIEW splus.v_z_udl_get_auth_values_cf AS
    SELECT agr_users.uname,
        agr_users.agr_name,
        agr_users.from_dat,
        agr_users.to_dat,
        agr_1251.object,
        agr_1251.field,
        ust12.von
    FROM (
        (
            {target_schema}.agr_users agr_users LEFT JOIN {target_schema}.agr_1251 agr_1251
                ON ((agr_1251.agr_name = agr_users.agr_name) AND (agr_1251.object = 'F_FICA_CTR'::bpchar) AND (agr_1251.field = 'FM_FICTR'::bpchar))
        ) LEFT JOIN {target_schema}.ust12 ust12
            ON ((ust12.objct = agr_1251.object) AND (agr_1251.auth = ust12.auth) AND (agr_1251.field = ust12.field))
    )
    WHERE ((agr_users.uname = 'VIDARD5'::bpchar) AND (agr_users.agr_name ~~ 'YP%'::text));

COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.uname IS 'Nom de l\'utilisateur ds fiche utilisateur';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.agr_name IS 'Nom du rôle';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.from_dat IS 'Date de validité (de)';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.to_dat IS 'Date de validité (à)';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.object IS 'Objet d\'autorisation';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.field IS 'Zone d\'autorisation';
COMMENT ON COLUMN splus.v_z_udl_get_auth_values_cf.von IS 'Valeur de l\'autorisation';