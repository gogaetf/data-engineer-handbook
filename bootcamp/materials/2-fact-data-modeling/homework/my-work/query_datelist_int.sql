WITH user_devices_selected AS (
    -- select last day in the month
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31')),
     date_series AS (
         -- supporting table for series of dates in the month
         SELECT *
         FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date),
     expand_series_for_int AS (
         -- assign a bit value when day of the month had activity
         SELECT user_id,
                device_id,
                browser_type,
                date,
                device_activity_datelist,
                CASE
                    WHEN device_activity_datelist @> ARRAY [DATE(series_date)]
                        THEN CAST(pow(2, 31 - (date - DATE(series_date))) AS bigint)
                    ELSE 0
                    END AS placeholder_int_value,
                DATE(series_date)
         FROM user_devices_selected
                  CROSS JOIN date_series),
     collapse_series_for_int_with_bit AS (
         -- collapse all bit values into a single integer + convert into bit(32)
         SELECT user_id,
                device_id,
                browser_type,
                MAX(device_activity_datelist) AS device_activity_datelist,
                MAX(placeholder_int_value) AS placeholder_int_value,
                CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
         FROM expand_series_for_int
         GROUP BY user_id, device_id, browser_type)
SELECT user_id,
       device_id,
       browser_type,
       datelist_int,
       BIT_COUNT(datelist_int) > 0 AS dim_is_monthly_active,
       BIT_COUNT(
               CAST('11111110000000000000000000000000' AS BIT(32)) &
               datelist_int
       ) > 0 AS dim_is_weekly_active,
       device_activity_datelist,
       placeholder_int_value
FROM collapse_series_for_int_with_bit
ORDER BY user_id, device_id, browser_type;

