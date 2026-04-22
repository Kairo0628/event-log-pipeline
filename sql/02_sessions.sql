CREATE TABLE sessions AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY session) AS session_id,
        session
    FROM (
        SELECT DISTINCT session
        FROM raw_event_log
    ) s
)