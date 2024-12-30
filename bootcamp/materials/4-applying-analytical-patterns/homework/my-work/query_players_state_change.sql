WITH years AS (
    SELECT *
        FROM generate_series(1996, 2022) AS season
),
     players_with_years AS (
         SELECT
             p.player_name,
             y.season,
             p.is_active,
             p.start_season,
             p.end_season
         FROM players_scd p
         JOIN years y
             ON y.season BETWEEN p.start_season AND p.end_season
     ),
    -- self join to compare the current season with the previous one
    players_self_joined AS (
        SELECT
            c.player_name,
            c.season,
            c.is_active AS is_active_season,
            y.is_active AS is_active_previous_season
        FROM players_with_years c
        LEFT JOIN players_with_years y
            ON c.player_name = y.player_name
            AND c.season = y.season + 1
    ),
    players_seasons_states AS(
        SELECT
            *,
            CASE
                WHEN is_active_season = TRUE
                    AND is_active_previous_season IS NULL THEN 'New'
                WHEN is_active_season = TRUE
                    AND is_active_previous_season = TRUE THEN 'Continued Playing'
                WHEN is_active_season = TRUE
                    AND is_active_previous_season = FALSE THEN 'Returned from Retirement'
                WHEN is_active_season = FALSE
                    AND is_active_previous_season = TRUE THEN 'Retired'
                ELSE 'Stayed Retired'
            END AS player_state
        FROM players_self_joined
    ),
    -- can also be done with WINDOW FUNCTION, and execution is faster
    players_state_changes AS (
        SELECT
            player_name,
            season,
            is_active AS is_active_season,
            LAG(is_active) OVER (PARTITION BY player_name ORDER BY season) AS is_active_previous_season
        FROM players_with_years
    ),
    players_seasons_states_window AS(
        SELECT
            *,
            CASE
                WHEN is_active_season = TRUE
                    AND is_active_previous_season IS NULL THEN 'New'
                WHEN is_active_season = TRUE
                    AND is_active_previous_season = TRUE THEN 'Continued Playing'
                WHEN is_active_season = TRUE
                    AND is_active_previous_season = FALSE THEN 'Returned from Retirement'
                WHEN is_active_season = FALSE
                    AND is_active_previous_season = TRUE THEN 'Retired'
                ELSE 'Stayed Retired'
            END AS player_state
        FROM players_state_changes
    )
SELECT
    *
FROM players_seasons_states_window
ORDER BY player_name,
    season;