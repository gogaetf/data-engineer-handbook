WITH teams_deduped AS (
    -- deduplicate teams due to dupicate rows
    SELECT
        team_id,
        nickname AS team_nickname
    FROM teams
    GROUP BY
        team_id,
        abbreviation,
        nickname
    ),
    game_details_deduped AS(
    -- deduplicate game_details due to dupicate rows
        SELECT
            gd.game_id,
            gd.team_id,
            gd.team_abbreviation,
            gd.player_id,
            gd.player_name,
            AVG(gd.pts) pts
        FROM game_details gd
        GROUP BY
            gd.game_id,
            gd.team_id,
            gd.team_abbreviation,
            gd.player_id,
            gd.player_name
    ),
    game_details_enriched AS (
        -- add game info to get winnig and losing teams and season
        SELECT
            gd.game_id,
            gd.team_id,
            gd.team_abbreviation,
            gd.player_id,
            gd.player_name,
            CASE WHEN gd.pts IS NULL THEN 0 ELSE gd.pts END AS pts,
            CASE
                WHEN gd.pts IS NULL OR gd.pts <= 10 THEN 0
                ELSE 1
            END AS pts_above_10,
            g.game_date_est,
            g.season,
            g.home_team_id,
            g.visitor_team_id,
            g.home_team_wins,
            CASE
            	WHEN g.home_team_wins = 1 AND gd.team_id = g.home_team_id
            	THEN 'winning team'
            	ELSE 'losing team'
            END win_lose_team
        FROM game_details_deduped gd
        JOIN games g
            ON gd.game_id = g.game_id
    ),
    games_grouped AS (
        -- group games to get number points and games won
        SELECT
            game_id,
            team_id,
            team_abbreviation,
            game_date_est,
            win_lose_team,
            SUM(CASE WHEN pts IS NULL THEN 0 ELSE pts END) AS total_points,
            CASE WHEN win_lose_team = 'winning team' THEN 1 ELSE 0 END AS win_game
        FROM game_details_enriched
        GROUP BY
            game_id,
            team_id,
            team_abbreviation,
            game_date_est,
            win_lose_team
    ),
    games_90_days AS (
        -- get total games won in a 90 day window
        SELECT
            *,
            SUM(win_game) OVER (
                PARTITION BY team_id
                ORDER BY game_date_est
                ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING
                ) AS total_games_won_90_days
        FROM games_grouped
    ),
    max_games_90_days AS (
        -- get maximum games won in a 90 day window for each team
        SELECT
            team_id,
            team_abbreviation,
            MAX(total_games_won_90_days) AS max_total_games_won_90_days
        FROM games_90_days
        GROUP BY
            team_id,
            team_abbreviation
        ORDER BY
            MAX(total_games_won_90_days) DESC
    ),
    question_1 AS (
        SELECT
            'Q1: What is the most games a team has won in a 90 game stretch?' AS question,
            max_total_games_won_90_days AS answer,
            team_abbreviation as additional_info
        FROM max_games_90_days
        LIMIT 1
    ),
    games_above_10 AS (
        -- add row number, detect where streak of any kind was broken
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, player_id
                ORDER BY game_date_est
                ) AS row_num_player,
            LAG(pts_above_10) OVER (
                PARTITION BY team_id, player_id
                ORDER BY game_date_est
                ) AS prev_pts_above_10,
            CASE
                WHEN LAG(pts_above_10) OVER (
                        PARTITION BY team_id, player_id
                        ORDER BY game_date_est
                        ) = pts_above_10
                    OR LAG(pts_above_10) OVER (
                        PARTITION BY team_id, player_id
                        ORDER BY game_date_est
                        ) IS NULL
                THEN 0
                ELSE 1
            END AS prev_pts_above_10_indicator
        FROM game_details_enriched
        WHERE player_name = 'LeBron James'
        ORDER BY
            team_id,
            team_abbreviation,
            player_id,
            player_name,
            game_date_est
    ),
    games_above_10_blocks AS(
        -- create blocks of strikes of any kind
        -- block numbers are not relevant
        -- they just represent row in which streak of any kind was broken
        SELECT
            team_id,
            team_abbreviation,
            player_id,
            game_date_est,
            pts_above_10,
            row_num_player,
            prev_pts_above_10_indicator,
            -- row number when the change has happened
            CASE
                WHEN prev_pts_above_10_indicator = 0 THEN 0
                ELSE row_num_player
            END AS row_num_player_change,
            MAX (CASE
                    WHEN prev_pts_above_10_indicator = 0 THEN 0
                    ELSE row_num_player
                END
            )
                OVER (PARTITION BY team_id, player_id
                        ORDER BY game_date_est
                        ROWS BETWEEN unbounded preceding AND CURRENT ROW
            ) AS block_change_row
        FROM games_above_10
        ORDER BY
            team_id,
            team_abbreviation,
            player_id,
            player_name,
            game_date_est
    ),
    streaks AS (
        -- aggregate streaks from blocks
        SELECT
            team_id,
            team_abbreviation,
            player_id,
            block_change_row,
            pts_above_10,
            COUNT(1) AS count_games_above_10_blocks
        FROM games_above_10_blocks
        WHERE pts_above_10 = 1
        GROUP BY
            team_id,
            team_abbreviation,
            player_id,
            block_change_row,
            pts_above_10
        ORDER BY COUNT(1) DESC
    ),
    question_2 AS (
        SELECT
            'Q2: How many games in a row did LeBron James score over 10 points a game?' AS question,
            count_games_above_10_blocks AS answer,
            team_abbreviation as additional_info
        FROM streaks
        LIMIT 1
    )
SELECT * FROM question_1
UNION ALL
SELECT * FROM question_2;
