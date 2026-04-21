CREATE TABLE staging_event_log AS (
    SELECT
        seq,
        user_id,
        device,
        os,
        browser,
        CASE
            WHEN url = 'mysite.co.kr' THEN 'main'
            WHEN url = 'mysite.co.kr/product' THEN 'product_main'
            WHEN REGEXP_MATCH(url, 'mysite\.co\.kr/product/[0-9]+') IS NOT NULL
                THEN CONCAT('product_', SPLIT_PART(url, '/', 3))
            ELSE 'payments'
        END AS page_type,
        s.session_id,
        event_type,
        event_timestamp,
        EXTRACT(MONTH FROM event_timestamp) AS month,
        EXTRACT(DAY FROM event_timestamp) AS day,
        EXTRACT(HOUR FROM event_timestamp) AS hour
    FROM raw_event_log l
    JOIN sessions s ON l.session = s.session
)
