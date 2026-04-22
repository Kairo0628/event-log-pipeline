DROP TABLE IF EXISTS mart_avg_session_duration;

CREATE TABLE mart_avg_session_duration AS (
    WITH session_duration AS (
        SELECT
            device,
            browser,
            session_id,
            EXTRACT(EPOCH FROM MAX(event_timestamp) - MIN(event_timestamp)) AS duration
        FROM staging_event_log
        GROUP BY 1, 2, 3
    )
    SELECT
        device,
        browser,
        ROUND(AVG(duration), 2) AS avg_session_duration_sec
    FROM session_duration
    GROUP BY 1, 2
    ORDER BY 3
)
