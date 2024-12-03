WITH deduplicate_game_details AS (
    SELECT
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id) AS row_num
    FROM game_details gd
)
SELECT
    *
FROM deduplicate_game_details
WHERE row_num = 1;

