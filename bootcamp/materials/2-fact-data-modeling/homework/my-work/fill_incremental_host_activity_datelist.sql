INSERT INTO hosts_cumulated
WITH max_end_date_history AS (SELECT DATE('2023-01-08') AS max_end_date),
     next_end_date_events AS (SELECT DATE('2023-01-09') AS next_event_date),
     history AS (SELECT *
                 FROM hosts_cumulated
                 WHERE date < (SELECT max_end_date FROM max_end_date_history)),
     last_day AS (SELECT *
                  FROM hosts_cumulated
                  WHERE date = (SELECT max_end_date FROM max_end_date_history)),
     new_events_aggregated AS (SELECT host,
                                      DATE(DATE_TRUNC('month', CAST(event_time AS TIMESTAMP))) AS month,
                                      DATE(CAST(event_time AS TIMESTAMP)) AS event_date,
                                      COUNT(1) AS num_events,
                                      COUNT(DISTINCT user_id) AS num_unique_visitors
                               FROM events
                               WHERE user_id IS NOT NULL
                                 AND DATE(CAST(event_time AS TIMESTAMP)) =
                                     (SELECT next_event_date FROM next_end_date_events)
                               GROUP BY host,
                                        DATE(DATE_TRUNC('month', CAST(event_time AS TIMESTAMP))),
                                        DATE(CAST(event_time AS TIMESTAMP))),
     combine_new_and_last_day AS (SELECT COALESCE(ld.host, nea.host) AS host,
                                         DATE(DATE_TRUNC(
                                                 'month',
                                                 COALESCE(nea.event_date,
                                                          (SELECT next_event_date FROM next_end_date_events))
                                              )) AS month,
                                         COALESCE(nea.event_date,
                                                  (SELECT next_event_date FROM next_end_date_events)) AS date,
                                         CASE
                                             -- user and host existed and had activity in history
                                             -- user and host have new activity
                                             WHEN host_activity_datelist IS NOT NULL
                                                 AND event_date IS NOT NULL
                                                 THEN host_activity_datelist || ARRAY [event_date]
                                             -- user and host existed and had activity in history
                                             -- user and host have no new activity
                                             WHEN host_activity_datelist IS NOT NULL
                                                 AND event_date IS NULL
                                                 THEN host_activity_datelist
                                             -- user and host did not exist in history
                                             -- user and host have new activity
                                             WHEN host_activity_datelist IS NULL
                                                 AND event_date IS NOT NULL
                                                 THEN ARRAY [event_date]
                                             END AS host_activity_datelist,
                                         (COALESCE(nea.num_events, 0) + COALESCE(ld.num_events_total, 0)) AS num_events_total,
                                         CASE
                                             -- user and host existed and had activity in history
                                             -- user and host have new activity
                                             WHEN num_events_list IS NOT NULL
                                                 AND num_events IS NOT NULL
                                                 THEN num_events_list || ARRAY [num_events]
                                             -- user and host existed and had activity in history
                                             -- user and host have no new activity
                                             WHEN num_events_list IS NOT NULL
                                                 AND num_events IS NULL
                                                 THEN num_events_list
                                             -- user and host did not exist in history
                                             -- user and host have new activity
                                             WHEN num_events_list IS NULL
                                                 AND num_events IS NOT NULL
                                                 THEN ARRAY [num_events]
                                             END AS num_events_list,
                                         CASE
                                             -- user and host existed and had activity in history
                                             -- user and host have new activity
                                             WHEN unique_visitors_list IS NOT NULL
                                                 AND num_unique_visitors IS NOT NULL
                                                 THEN unique_visitors_list || ARRAY [num_unique_visitors]
                                             -- user and host existed and had activity in history
                                             -- user and host have no new activity
                                             WHEN unique_visitors_list IS NOT NULL
                                                 AND num_unique_visitors IS NULL
                                                 THEN unique_visitors_list
                                             -- user and host did not exist in history
                                             -- user and host have new activity
                                             WHEN unique_visitors_list IS NULL
                                                 AND num_unique_visitors IS NOT NULL
                                                 THEN ARRAY [num_unique_visitors]
                                             END AS unique_visitors_list
                                  FROM new_events_aggregated nea
                                           FULL OUTER JOIN last_day ld
                                                           ON nea.host = ld.host)
SELECT host,
       month,
       date,
       host_activity_datelist,
       num_events_total,
       num_events_list,
       unique_visitors_list
FROM combine_new_and_last_day
ON CONFLICT (host, month, date)
    DO UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist,
                  num_events_total       = EXCLUDED.num_events_total,
                  num_events_list        = EXCLUDED.num_events_list,
                  unique_visitors_list   = EXCLUDED.unique_visitors_list;

-- SELECT * FROM hosts_cumulated ;
