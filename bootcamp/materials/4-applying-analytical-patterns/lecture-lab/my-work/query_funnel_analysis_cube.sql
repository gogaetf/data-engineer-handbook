WITH events_augmented AS (
    SELECT
        COALESCE(d.os_type, 'Unknown') AS os_type,
        COALESCE(d.device_type, 'Unknown') AS device_type,
        COALESCE(d.browser_type, 'Unknown') AS browser_type,
        url,
        user_id
    FROM events e
    JOIN devices d
        ON e.device_id = d.device_id
)
SELECT
    GROUPING(os_type) AS os_type_grouping,
    GROUPING(device_type) AS device_type_grouping,
    GROUPING(browser_type) AS browser_type_grouping,
    CASE
        WHEN GROUPING(os_type) = 0
            AND GROUPING(device_type) = 0
            AND GROUPING(browser_type) = 0
        THEN 'os_type__device_type__browser_type'
        WHEN GROUPING(os_type) = 0 THEN 'os_type'
        WHEN GROUPING(device_type) = 0 THEN 'device_type'
        WHEN GROUPING(browser_type) = 0 THEN 'browser_type'
    END AS aggregation_level,
    COALESCE(os_type, '(overall)') AS os_type,
    COALESCE(device_type, '(overall)') AS device_type,
    COALESCE(browser_type, '(overall)') AS browser_type,
    COUNT(1)
FROM events_augmented
GROUP BY CUBE(browser_type, device_type, os_type)
ORDER BY COUNT(1) DESC;