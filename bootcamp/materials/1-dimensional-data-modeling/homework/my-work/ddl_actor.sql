CREATE TYPE film_stats AS
(
    film_id text,
    film    text,
    votes   integer,
    rating  real
);

CREATE TYPE quality_class
AS enum ('star', 'good', 'average', 'bad');

-- DROP TABLE IF EXISTS actors;

CREATE TABLE actors
(
    actor_id              text,
    actor_name            text,
    year                  integer,
    films                 film_stats[],
    quality_class         quality_class,
    is_active             boolean,
    first_year            integer,
    last_film_year        integer,
    years_since_last_film integer,
    PRIMARY KEY (actor_id, year)
);

