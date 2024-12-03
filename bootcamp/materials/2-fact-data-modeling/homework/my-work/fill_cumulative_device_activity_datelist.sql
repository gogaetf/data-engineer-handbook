INSERT INTO user_devices_cumulated
WITH
    -- series of days for the existing month of data
    date_series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
    ),
    -- reducing events to a daily count
    events_grouped AS(
        SELECT
            user_id,
            device_id,
            DATE(CAST(event_time AS TIMESTAMP)) AS event_date,
            COUNT(1) AS num_events
        FROM events
        WHERE user_id IS NOT NULL
        GROUP BY user_id, device_id, DATE(CAST(event_time AS TIMESTAMP))
    ),
    -- explode events into a daily series
    events_explode AS(
    SELECT
        e.user_id,
        e.device_id,
        e.event_date,
        DATE(ds.series_date ) as date
    FROM events_grouped e
    JOIN date_series ds
        ON e.event_date <= DATE(ds.series_date)
    WHERE e.user_id IS NOT NULL
    ),
    -- aggregate events for each day of the month cumulatively
    events_cumulative AS (
        SELECT
            user_id,
            device_id,
            date,
            array_agg(event_date) AS device_activity_datelist
        FROM events_explode
        GROUP BY user_id, device_id, date
    ),
    -- deduplicate devices
    devices_deduplicated AS (
        SELECT
            device_id,
            device_type,
            browser_type
        FROM devices
        GROUP BY device_id, device_type, browser_type
    ),
    -- join events with devices
    events_cumulative_device AS (
        SELECT
            ec.user_id,
            ec.device_id,
            dd.browser_type,
            ec.date,
            ec.device_activity_datelist
        FROM events_cumulative ec
                 JOIN devices_deduplicated dd
                      ON ec.device_id = dd.device_id
    )
SELECT
    user_id,
    device_id,
    browser_type,
    date,
    device_activity_datelist
FROM events_cumulative_device
ON CONFLICT (user_id, device_id, browser_type, date)
DO UPDATE
    SET device_activity_datelist = EXCLUDED.device_activity_datelist;

