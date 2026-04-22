DROP TABLE IF EXISTS mart_conversion_rate;

CREATE TABLE mart_conversion_rate AS (
    WITH session_inflow AS (
        SELECT
            session_id,
            user_id,
            CASE
                WHEN event_type = 'main' THEN 'organic'
                ELSE 'ad'
            END AS inflow_type
        FROM (
            SELECT
                session_id,
                user_id,
                event_type,
                ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_timestamp) AS session_row_num
            FROM staging_event_log
        ) s
        WHERE session_row_num = 1
    ), session_purchase AS (
        SELECT
            session_id,
            SUM(CASE
                    WHEN event_type = 'purchase' THEN 1
                    ELSE 0
                END) AS purchase_count,
            SUM(CASE
                    WHEN event_type = 'success' THEN 1
                    ELSE 0
                END) AS success_count
        FROM staging_event_log
        GROUP BY 1
    )
    SELECT
        inflow_type,
        COUNT(DISTINCT user_id) AS user_count,
        COUNT(DISTINCT i.session_id) AS session_count,
        SUM(purchase_count) AS purchase_count,
        ROUND(SUM(purchase_count)/ CAST(COUNT(DISTINCT i.session_id) AS NUMERIC) * 100, 2) AS purchase_rate,
        SUM(success_count) AS success_count,
        ROUND(SUM(success_count)/ CAST(COUNT(DISTINCT i.session_id) AS NUMERIC) * 100, 2) AS success_rate
    FROM session_inflow i
    JOIN session_purchase p ON i.session_id = p.session_id
    GROUP BY 1
)