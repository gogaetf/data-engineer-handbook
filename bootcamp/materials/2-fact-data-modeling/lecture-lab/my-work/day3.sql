--drop table IF EXISTS array_metrics;

DELETE FROM array_metrics;

 CREATE TABLE array_metrics (
     user_id NUMERIC,
     month_start DATE,
     metric_name TEXT,
     metric_array REAL[],
     PRIMARY KEY (user_id, month_start, metric_name)
 )


-- INSERT INTO array_metrics
-- WITH daily_aggregate AS (
--     SELECT
--         user_id,
--         DATE(event_time) AS date,
--         count(1) AS num_site_hits
--     FROM
--         events
--     WHERE DATE(event_time) = DATE('2023-01-31')
--     AND user_id IS NOT NULL
--     GROUP BY
--         user_id,
--         DATE(event_time)
-- ),
--     yesterday_array AS (
--         SELECT
--             *
--         FROM
--             array_metrics
--         WHERE month_start = DATE('2023-01-02')
--     )
-- SELECT
--     coalesce(da.user_id, ya.user_id) AS user_id,
--     coalesce(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
--     'site_hits' AS metric_name,
--     CASE
--         WHEN ya.metric_array IS NOT NULL
--             THEN ya.metric_array || ARRAY[coalesce(da.num_site_hits, 0)]
--         WHEN ya.month_start IS NOT NULL
--             THEN ARRAY[coalesce(da.num_site_hits, 0)]
--         WHEN ya.metric_array IS NULL
--             THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)])
--                      || ARRAY[coalesce(da.num_site_hits, 0)]
--     END AS metric_array
-- FROM
--     daily_aggregate da
-- FULL OUTER JOIN yesterday_array ya
--     ON da.user_id = ya.user_id
-- ON CONFLICT (user_id, month_start, metric_name)
-- DO
--     UPDATE
--     SET metric_array = EXCLUDED.metric_array;

SELECT * FROM array_metrics;

SELECT cardinality(metric_array) , COUNT(1)
from array_metrics
GROUP BY 1;

-- bring it back to daily aggregate with
WITH nested_agg AS (
    SELECT
        metric_name,
        month_start,
        -- first aggregated
        ARRAY[
            SUM(metric_array[1]),
            SUM(metric_array[2]),
            SUM(metric_array[3]),
            SUM(metric_array[4]) -- add as many days as there are in the month
            ] AS summed_array
    FROM array_metrics
    GROUP BY
        metric_name,
        month_start
)
SELECT
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL) AS date,
    elem AS value
FROM nested_agg
-- unnest after aggregation
CROSS JOIN unnest(nested_agg.summed_array)
WITH ORDINALITY  AS a(elem, index);




