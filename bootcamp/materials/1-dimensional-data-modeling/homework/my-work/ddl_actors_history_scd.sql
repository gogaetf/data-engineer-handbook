
-- drop table actors_history_scd ;

create table actors_history_scd
(
    actor_id text,
    actor_name text,
    quality_class quality_class,
    is_active boolean,
    first_year integer,
    start_film_year integer,
    end_film_year integer,
    primary key (actor_id, start_film_year)
);


