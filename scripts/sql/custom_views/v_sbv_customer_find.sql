CREATE VIEW splus.v_sbv_customer_find AS
    SELECT kunnr,
        name1
    FROM {target_schema}.kna1;