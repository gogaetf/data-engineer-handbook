-- Active: 1735323009064@@host.docker.internal@5432@postgres@public
-- what percentage of users that got to the signup page actually signed up?
-- will be done without window functions, but with a self join

--select distinct url from events where url like '%user%' order by url;

-- there is a difference in counting conversion rates
-- option 1: conversion of users - will give optimistic numbers
-- option 2 conversion of hits - will give ore realistic numbers
WITH deduped_events AS (
    SELECT
        user_id,
        url,
        event_time,
        date(event_time) AS event_date
    FROM events
    WHERE
        user_id IS NOT NULL
    --    AND url in ('/signup', '/api/v1/user')
    --    AND url in ('/signup', '/api/v1/login')
    GROUP BY
        user_id,
        url,
        event_time,
        date(event_time)
),
    selfjoined AS(
        SELECT
            d1.user_id,
            d1.url,
            d2.url AS destination_url,
            d1.event_time,
            date(d1.event_time) AS event_date_d1,
            d2.event_time AS event_time_destination,
            date(d2.event_time) AS event_date_destination
        FROM deduped_events d1
        JOIN deduped_events d2
            ON d1.user_id = d2.user_id
            AND d1.event_date = d2.event_date
            AND d1.event_time < d2.event_time
        -- WHERE d1.url = '/signup'
            -- AND d2.url = '/api/v1/login'
    ),
    userlevel AS (
        SELECT
            user_id,
            url,
            COUNT(1) AS num_hits,
            SUM(CASE WHEN destination_url = '/api/v1/login' THEN 1 ELSE 0 END) AS converted
        FROM selfjoined
        GROUP BY
            user_id,
            url
    )
SELECT
    url,
    SUM(num_hits) as num_hits,
    SUM(converted) AS num_converted,
    CAST(SUM(converted) AS REAL) / SUM(num_hits) AS pct_converted
FROM userlevel
GROUP BY
    url
HAVING SUM(num_hits) > 500
ORDER BY CAST(SUM(converted) AS REAL) / SUM(num_hits);
