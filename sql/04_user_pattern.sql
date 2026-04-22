DROP TABLE IF EXISTS mart_user_pattern;

CREATE TABLE mart_user_pattern AS (
    WITH session_event_detail AS (
        SELECT
            user_id,
            session_id,
            COUNT(*) OVER(PARTITION BY session_id) AS session_event_count,
            CASE
                WHEN event_type = 'success' THEN 1 ELSE 0
            END AS is_success,
            MAX(CASE
                    WHEN event_type = 'success' THEN 1 ELSE 0
                END) OVER(PARTITION BY session_id) AS session_has_success
        FROM staging_event_log
    ), user_summary AS (
        SELECT
            user_id,
            session_id,
            MAX(session_event_count) AS session_event_count,
            MAX(session_has_success) AS session_has_success,
            SUM(is_success) AS session_success_count
        FROM session_event_detail
        GROUP BY 1, 2
    )
    SELECT
        user_id,
        COUNT(session_id) AS session_count,
        SUM(session_has_success) AS total_success_session,
        SUM(session_event_count) AS total_user_event,
        SUM(session_success_count) AS total_success_event,
        ROUND(AVG(session_event_count), 2) AS avg_event_count
    FROM user_summary
    GROUP BY 1
)