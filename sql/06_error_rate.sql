DROP TABLE IF EXISTS mart_error_rate;

CREATE TABLE mart_error_rate AS (
    WITH total_purchase AS (
        SELECT
            device,
            os,
            browser,
            COUNT(*) AS purchase_count
        FROM staging_event_log
        WHERE event_type = 'purchase'
        GROUP BY 1, 2, 3
    ), error_rate AS (
        SELECT
            device,
            os,
            browser,
            COUNT(*) AS error_count
        FROM staging_event_log
        WHERE event_type = 'fail'
        GROUP BY 1, 2, 3
    )
    SELECT
        e.device,
        e.os,
        e.browser,
        purchase_count,
        COALESCE(error_count, 0) AS error_count,
        ROUND(COALESCE(error_count, 0) / CAST(purchase_count AS NUMERIC) * 100, 2) AS error_rate
    FROM error_rate e
    LEFT JOIN total_purchase p
    ON e.device = p.device
    AND e.os = p.os
    AND e.browser = p.browser
    ORDER BY 6 DESC
)