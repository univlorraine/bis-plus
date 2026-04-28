DROP VIEW IF EXISTS splus.reporting_csks;
CREATE VIEW splus.reporting_csks AS
    SELECT c.bukrs, c.kostl, c.datbi, p.posid
    FROM {target_schema}.csks c
    JOIN {target_schema}.prps p ON c.bukrs = p.bukrs
    WHERE c.datbi > CURRENT_DATE;