INSERT INTO actors_history_scd
WITH indicator_change AS (SELECT *,
                                 CASE
                                     WHEN LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) IS NULL
                                         THEN 0
                                     WHEN quality_class =
                                          LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) THEN 0
                                     ELSE 1
                                     END AS quality_class_change,
                                 CASE
                                     WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year) IS NULL THEN 0
                                     WHEN is_active = LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year)
                                         THEN 0
                                     ELSE 1
                                     END AS is_active_change,
                                 MAX(year) OVER (PARTITION BY actor_id) AS max_year
                          FROM actors
--                           WHERE year <= 2023
                          ),
     indicators_change_combined AS (SELECT *,
                                           CASE
                                               WHEN quality_class_change = 1 OR is_active_change = 1 THEN 1
                                               ELSE 0
                                               END AS indicators_change
                                    FROM indicator_change),
     cumulative_indicators_change AS (SELECT *,
                                             SUM(indicators_change) OVER (PARTITION BY actor_id ORDER BY year) AS cumulative_indicators_change
                                      FROM indicators_change_combined),
     fill_actors_history_scd AS (SELECT actor_id,
                                        actor_name,
                                        quality_class,
                                        is_active,
                                        first_year,
                                        MIN(year) AS start_film_year,
                                        MAX(year) AS end_film_year
                                 FROM cumulative_indicators_change
                                 GROUP BY actor_id, actor_name, cumulative_indicators_change, quality_class, is_active,
                                          first_year
                                 ORDER BY actor_id, actor_name, cumulative_indicators_change)

SELECT *
FROM fill_actors_history_scd
ORDER BY actor_id, actor_name, start_film_year, end_film_year;

