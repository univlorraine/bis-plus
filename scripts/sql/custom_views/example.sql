DROP VIEW IF EXISTS splus.reporting_csks;
CREATE VIEW splus.reporting_csks AS
    SELECT c.bukrs, c.kostl, c.datbi, p.posid
    FROM {target_schema}.csks AS c
    CROSS JOIN {target_schema}.prps AS p;