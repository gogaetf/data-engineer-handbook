INSERT INTO host_activity_reduced_monthly
WITH current_month AS (SELECT DATE('2022-12-01') AS current_month),
     next_month AS (SELECT DATE('2023-01-01') AS next_month),
     history AS (SELECT *
                 FROM hosts_cumulated
                 WHERE month < (SELECT current_month FROM current_month)),
     current_month_hosts_cumulated AS (SELECT *
                                       FROM host_activity_reduced_monthly
                                       WHERE month = (SELECT current_month FROM current_month)),
     new_hosts_cumulated AS (SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY host, month ORDER BY date DESC) AS row_number
                             FROM hosts_cumulated
                             WHERE month = (SELECT next_month FROM next_month)),
     last_new_hosts_cumulated AS (SELECT *
                                  FROM new_hosts_cumulated
                                  WHERE row_number = 1),
     date_series AS (
         -- supporting table for series of dates in the month
         SELECT *
         FROM GENERATE_SERIES(
                      (SELECT next_month FROM next_month),
                      ((SELECT next_month FROM next_month) +
                       INTERVAL '1 month' - INTERVAL '1 day'),
                      INTERVAL '1 day') AS series_date),
     expand_series_for_int AS (
         -- assign a bit value when day of the month had activity
         SELECT host,
                month,
                host_activity_datelist,
                num_events_total,
                num_events_list,
                unique_visitors_list,
                CASE
                    WHEN host_activity_datelist @> ARRAY [DATE(series_date)]
                        THEN CAST(pow(
                            2,
                            31 - (DATE((SELECT next_month FROM next_month) +
                                       INTERVAL '1 month' - INTERVAL '1 day') - DATE(series_date)))
                        AS bigint)
                    ELSE 0
                    END AS placeholder_int_value,
                DATE(series_date)
         FROM last_new_hosts_cumulated
                  CROSS JOIN date_series),
     collapse_series_for_int_with_bit AS (
         -- collapse all bit values into a single integer + convert into bit(32)
         SELECT host,
                month,
                MAX(host_activity_datelist) AS host_activity_datelist,
                CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS host_activity_datelist_int,
                MAX(num_events_list) AS num_events_list,
                MAX(unique_visitors_list) AS unique_visitors_list,
                MAX(num_events_total) AS num_events_total
         FROM expand_series_for_int
         GROUP BY host, month)
SELECT host,
       month,
       host_activity_datelist,
       host_activity_datelist_int,
       num_events_list,
       unique_visitors_list,
       num_events_total
FROM collapse_series_for_int_with_bit
ON CONFLICT (host, month)
    DO UPDATE
    SET host_activity_datelist     = EXCLUDED.host_activity_datelist,
        host_activity_datelist_int = EXCLUDED.host_activity_datelist_int,
        num_events_list            = EXCLUDED.num_events_list,
        unique_visitors_list       = EXCLUDED.unique_visitors_list,
        num_events_total           = EXCLUDED.num_events_total;

-- SELECT * FROM host_activity_reduced_monthly;

