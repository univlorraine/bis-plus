CREATE VIEW splus.v_z_udl_get_auth_values_tr AS
    SELECT agr_users.uname,
        agr_users.agr_name,
        agr_users.from_dat,
        agr_users.to_dat,
        agr_tcodes.tcode
    FROM (
        {target_schema}.agr_users agr_users LEFT JOIN {target_schema}.agr_tcodes agr_tcodes
            ON ((agr_tcodes.agr_name = agr_users.agr_name) AND (agr_tcodes.type = 'TR'::bpchar)))
    WHERE (agr_users.agr_name ~~ 'YS%'::text);