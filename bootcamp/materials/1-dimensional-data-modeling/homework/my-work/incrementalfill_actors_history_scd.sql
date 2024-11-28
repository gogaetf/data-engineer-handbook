INSERT INTO actors_history_scd
WITH max_end_year_historical AS (SELECT MAX(end_film_year) AS max_end_year
                                 FROM actors_history_scd),
     historical_records_scd AS (SELECT actor_id,
                                       actor_name,
                                       quality_class,
                                       is_active,
                                       first_year,
                                       start_film_year,
                                       end_film_year
                                FROM actors_history_scd
                                WHERE end_film_year < (SELECT max_end_year FROM max_end_year_historical)),
     last_records_scd AS (SELECT actor_id,
                                 actor_name,
                                 quality_class,
                                 is_active,
                                 first_year,
                                 start_film_year,
                                 end_film_year
                          FROM actors_history_scd
                          WHERE end_film_year = (SELECT max_end_year FROM max_end_year_historical)),
     new_actors_records AS (SELECT actor_id,
                                   actor_name,
                                   quality_class,
                                   is_active,
                                   first_year,
                                   year AS start_film_year,
                                   year AS end_film_year
                            FROM actors
                            WHERE year > (SELECT max_end_year FROM max_end_year_historical)),
     union_last_new AS (SELECT *
                        FROM last_records_scd
                        UNION ALL
                        SELECT *
                        FROM new_actors_records),
     -- assumption: maybe the initial insert was run regularly, but maybe it was not
     -- we do not know if there is one year is added or updated in actors table
     indicator_change AS (SELECT actor_id,
                                 actor_name,
                                 quality_class,
                                 is_active,
                                 first_year,
                                 start_film_year,
                                 end_film_year,
                                 CASE
                                     WHEN LAG(quality_class, 1)
                                          OVER (PARTITION BY actor_id ORDER BY start_film_year) IS NULL
                                         THEN 0
                                     WHEN quality_class =
                                          LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY start_film_year)
                                         THEN 0
                                     ELSE 1
                                     END AS quality_class_change,
                                 CASE
                                     WHEN LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY start_film_year) IS NULL
                                         THEN 0
                                     WHEN is_active =
                                          LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY start_film_year)
                                         THEN 0
                                     ELSE 1
                                     END AS is_active_change
                          FROM union_last_new),
     indicators_change_combined AS (SELECT *,
                                           CASE
                                               WHEN quality_class_change = 1 OR is_active_change = 1 THEN 1
                                               ELSE 0
                                               END AS indicators_change
                                    FROM indicator_change),
     cumulative_indicators_change AS (SELECT *,
                                             SUM(indicators_change)
                                             OVER (PARTITION BY actor_id ORDER BY start_film_year) AS cumulative_indicators_change
                                      FROM indicators_change_combined),
     fill_partial_history_scd AS (SELECT actor_id,
                                         actor_name,
                                         quality_class,
                                         is_active,
                                         first_year,
                                         MIN(start_film_year) AS start_film_year,
                                         MAX(end_film_year) AS end_film_year
                                  FROM cumulative_indicators_change
                                  GROUP BY actor_id, actor_name, cumulative_indicators_change, quality_class, is_active,
                                           first_year
                                  ORDER BY actor_id, actor_name, cumulative_indicators_change)
SELECT actor_id,
       actor_name,
       quality_class,
       is_active,
       first_year,
       start_film_year,
       end_film_year
FROM fill_partial_history_scd
ON CONFLICT (actor_id, start_film_year) DO UPDATE SET end_film_year = EXCLUDED.end_film_year;

