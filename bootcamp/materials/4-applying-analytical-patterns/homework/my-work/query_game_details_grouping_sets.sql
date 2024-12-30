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
            g.season,
            g.home_team_id,
            g.visitor_team_id,
            g.home_team_wins,
            CASE
            	WHEN g.home_team_wins = 1 AND gd.team_id = g.home_team_id
            	THEN 'winning team'
            	ELSE 'losing team'
            END Win_lose_team
        FROM game_details_deduped gd
        JOIN games g
            ON gd.game_id = g.game_id
    ),
    grouping_checks AS (
        -- used during development to check grouping sets results
        SELECT
--            player_id, player_name, team_id, team_abbreviation,
--            player_id, player_name, season,
            team_id, team_abbreviation,
            SUM(CASE WHEN pts IS NULL THEN 0 ELSE pts END) AS total_points,
            COUNT(DISTINCT CASE WHEN Win_lose_team = 'winning team' THEN game_id ELSE NULL END) AS games_won
        FROM game_details_enriched
        GROUP BY
--            player_id, player_name, team_id, team_abbreviation
--            player_id, player_name, season
            team_id, team_abbreviation
    ),
    -- grouping sets for Q1, Q2, Q3
    grouping_stats_q1 AS (
        SELECT
            GROUPING(player_id) AS grouping_player_id,
            GROUPING(team_id) AS grouping_team_id,
            GROUPING(season) AS grouping_season,
            COALESCE(CAST(player_id AS text), '(overall)') AS player_id,
            COALESCE(player_name, '(overall)') AS player_name,
            COALESCE(CAST(team_id AS text), '(overall)') AS team_id,
            COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
            COALESCE(CAST(season AS text), '(overall)') AS season,
            SUM(CASE WHEN pts IS NULL THEN 0 ELSE pts END) AS total_points,
            COUNT(DISTINCT CASE WHEN Win_lose_team = 'winning team' THEN game_id ELSE NULL END) AS games_won
        FROM game_details_enriched
        GROUP BY GROUPING SETS (
            (player_id, player_name, team_id, team_abbreviation),
            (player_id, player_name, season),
            (team_id, team_abbreviation)
            )
    ),
    question_1 AS (
        SELECT
            'Q1: Who scored the most points playing for one team?' AS question,
            *
        FROM grouping_stats_q1
        WHERE season = '(overall)' AND player_id <> '(overall)' AND team_id <> '(overall)'
        ORDER BY total_points DESC
        LIMIT 1
    ),
    question_2 AS (
        SELECT
            'Q2: Who scored the most points in one season?' AS question,
            *
        FROM grouping_stats_q1
        WHERE team_id = '(overall)' AND season <> '(overall)' AND player_id <> '(overall)'
        ORDER BY total_points DESC
        LIMIT 1
    ),
    question_3 AS (
        SELECT
            'Q3: Which team has won the most games?' AS question,
            *
        FROM grouping_stats_q1
        WHERE season = '(overall)' AND player_id = '(overall)'
        ORDER BY games_won DESC
        LIMIT 1
    )
SELECT * FROM question_1
UNION
SELECT * FROM question_2
UNION
SELECT * FROM question_3;
