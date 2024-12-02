SELECT *
FROM game_details
ORDER BY game_id, team_id, player_id;

SELECT game_id,
       team_id,
       player_id,
       COUNT(1)
FROM game_details
GROUP BY 1, 2, 3
HAVING COUNT(1) > 1;

-- CREATE TABLE fct_game_details (
--     dim_game_date DATE,
--     dim_season INTEGER,
--     dim_team_id INTEGER,
--     dim_player_id INTEGER,
--     dim_player_name TEXT,
--     dim_start_position TEXT,
--     dim_is_playing_at_home BOOLEAN,
--     dim_not_play BOOLEAN,
--     dim_did_not_dress BOOLEAN,
--     dim_not_with_team BOOLEAN,
--     m_minutes REAL,
--     m_fgm INTEGER,
--     m_fga INTEGER,
--     m_fg3m INTEGER,
--     m_fg3a INTEGER,
--     m_ftm INTEGER,
--     m_fta INTEGER,
--     m_oreb INTEGER,
--     m_dreb INTEGER,
--     m_ast INTEGER,
--     m_stl INTEGER,
--     m_blk INTEGER,
--     m_turnovers INTEGER,
--     m_pf INTEGER,
--     m_pts INTEGER,
--     m_plus_minus INTEGER,
--     PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
-- );

INSERT INTO fct_game_details
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
--         g.visitor_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id, game_date_est ORDER BY game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    coalesce(position('DNP' IN comment), 0) > 0 AS dim_not_play,
    coalesce(position('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    coalesce(position('NWT' IN comment), 0) > 0 AS dim_not_with_team,
--     comment, --probably all is parsed and column is not needed any more
--     min,
    CAST(split_part(min, ':', 1) AS REAL)
    + CAST(split_part(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1
ORDER BY game_id, team_id, player_id;

SELECT *
FROM fct_game_details fgd
JOIN teams t ON fgd.dim_team_id = t.team_id;

SELECT
    dim_player_name,
    count(1) AS num_games,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END ) AS bailed_num,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END ) AS REAL) / count(1) AS bailed_pct
FROM fct_game_details
GROUP BY dim_player_name
ORDER BY 4 DESC;
