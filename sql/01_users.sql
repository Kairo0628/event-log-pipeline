CREATE TABLE users AS (
    SELECT DISTINCT user_id, user_name
    FROM raw_event_log
)