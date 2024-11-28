INSERT INTO actors
WITH agg_actor AS (SELECT actor,
                          actorid,
                          MIN(year) AS first_year
                   FROM actor_films
                   GROUP BY actor,
                            actorid),
     years AS (SELECT *
               FROM GENERATE_SERIES(1970, 2024) AS year),
     actors_films_years AS (SELECT ac.actor,
                                   ac.actorid,
                                   ac.first_year,
                                   y.year,
                                   af.year AS film_year,
                                   af.filmid,
                                   af.film,
                                   af.votes,
                                   af.rating
                            FROM agg_actor ac
                                     LEFT JOIN years y
                                               ON y.year >= ac.first_year
                                     LEFT JOIN actor_films af
                                               ON ac.actor = af.actor
                                                   AND af.year <= y.year
                            ORDER BY actor, year, film_year, film),
     aggregated AS (SELECT actor,
                           actorid,
                           first_year,
                           year,
                           ARRAY_AGG(ROW (
                               filmid,
                               film,
                               votes,
                               rating)::film_stats) AS films,
                           ARRAY_AGG(ROW (
                               film_year,
                               filmid,
                               film,
                               votes,
                               rating)) AS films_year,
                           MAX(film_year) AS last_film_year
                    FROM actors_films_years
                    GROUP BY actor,
                             actorid,
                             first_year,
                             year)
SELECT actorid,
       actor,
       year,
       films,
       (CASE
            WHEN (films[CARDINALITY(films)]::film_stats).rating > 8 THEN 'star'
            WHEN (films[CARDINALITY(films)]::film_stats).rating > 7 THEN 'good'
            WHEN (films[CARDINALITY(films)]::film_stats).rating > 6 THEN 'average'
            ELSE 'bad'
           END)::quality_class AS quality_class,
       CASE WHEN last_film_year = year THEN TRUE ELSE FALSE END AS is_active,
       first_year,
       last_film_year,
       year - last_film_year AS years_since_last_film
FROM aggregated
ORDER BY actor, year;

