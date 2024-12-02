SELECT
    max(event_time),
    min(event_time)
FROM events;

-- DROP TABLE IF EXISTS users_cumulated;

-- CREATE TABLE users_cumulated (
--     user_id text,
--     -- The list of dates in the past where the user was active
--     dates_active DATE[],
--     -- The current date for the user
--     date DATE,
--     PRIMARY KEY (user_id, date)
-- );

-- INSERT INTO users_cumulated
-- WITH yesterday AS (
--     SELECT
--         *
--     FROM  users_cumulated
--     WHERE date = DATE('2023-01-30')
-- ),
--      today AS (
--          SELECT
--              CAST(user_id AS text) AS user_id,
--              DATE(CAST(event_time AS TIMESTAMP)) AS date_active
--          FROM  events
--          WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
--             AND user_id IS NOT NULL
--          GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
--      )
-- SELECT
--     coalesce(t.user_id, y.user_id) AS user_id,
--     CASE
--         WHEN y.dates_active IS NULL
--         THEN ARRAY[t.date_active]
--         WHEN t.date_active IS NULL
--         THEN y.dates_active
--         ELSE ARRAY[t.date_active] || y.dates_active
--     END AS dates_active,
--     coalesce(t.date_active, y.date + INTERVAL '1 day') as date
-- FROM today t
-- FULL OUTER JOIN yesterday y
--     ON t.user_id = y.user_id;

SELECT *
FROM users_cumulated
WHERE date = DATE('2023-01-31');

SELECT * FROM generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 day') AS valid_date;

WITH users AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2023-01-31')
),
    series AS (
        SELECT *
        FROM generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
    ),
    place_holder_ints AS (
        SELECT
            CASE
                WHEN dates_active @> ARRAY [DATE(series_date)]
                    THEN CAST(pow(2, 32 - (date - DATE(series_date))) AS bigint)
                ELSE 0
                END AS placeholder_int_value,
            *
        FROM users
        CROSS JOIN series
    )
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int,
    BIT_COUNT(
            CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
    ) > 0 AS dim_is_monthly_active,
    BIT_COUNT(
            CAST('11111110000000000000000000000000' AS BIT(32)) &
              CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
    ) > 0 AS dim_is_weekly_active,
    BIT_COUNT(
            CAST('10000000000000000000000000000000' AS BIT(32)) &
            CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
    ) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id ;


