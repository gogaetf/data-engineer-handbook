SELECT * FROM player_seasons;

-- create type season_stats as (
--     season integer,
--     gp integer,
--     pts real,
--     reb real,
--     ast real
--     );

-- create type scoring_class as enum ('star', 'good', 'average', 'bad');
--

-- drop table players;

-- changed on day 2

-- create table players(
--     player_name text,
--     height text,
--     college text,
--     country text,
--     draft_year text,
--     draft_round text,
--     draft_number text,
--     season_stats season_stats[],
--     scoring_class scoring_class,
--     years_since_last_season integer,
--     current_season integer,
--     is_active boolean,
--     primary key (player_name, current_season)
-- );
-- changed on day 2
-- insert into players
-- with years as (
--     select *
--         from generate_series(1996, 2022) as season
-- ),
--     p as (
--         select player_name, min(season) as first_season
--         from player_seasons
--         group by player_name
--     ),
--     players_and_seasons as (
--         select *
--         from p
--         join years y on p.first_season <= y.season
--     ),
--     windowed as (
--         select
--             ps.player_name,
--             ps.season,
--             array_remove(
--                 array_agg(
--                     case
--                         when p1.season is not null
--                         then row(
--                             p1.season,
--                             p1.gp,
--                             p1.pts,
--                             p1.reb,
--                             p1.ast
--                         )::season_stats
--                     end
--                 ) over (partition by ps.player_name order by coalesce(ps.season, p1.season)),
--                 null
--             ) as seasons
--         from players_and_seasons ps
--         left join player_seasons p1
--             on ps.player_name = p1.player_name
--             and ps.season = p1.season
--         order by ps.player_name, ps.season
--     ),
--     static as (
--         select
--             player_name,
--             max(height) as height,
--             max(college) as college,
--             max(country) as country,
--             max(draft_year) as draft_year,
--             max(draft_round) as draft_round,
--             max(draft_number) as draft_number
--         from player_seasons
--         group by player_name
--     )
-- select
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons as season_stats,
--     case
--         when (seasons[cardinality(seasons)]::season_stats).pts > 20 then 'star'
--         when (seasons[cardinality(seasons)]::season_stats).pts > 15 then 'good'
--         when (seasons[cardinality(seasons)]::season_stats).pts > 10 then 'average'
--         else 'bad'
--     end::scoring_class as scoring_class,
--     w.season - (seasons[cardinality(seasons)]::season_stats).season as years_since_last_season,
--     w.season,
--     (seasons[cardinality(seasons)]::season_stats).season = w.season as is_active
-- from windowed w
-- join static s
--     on w.player_name = s.player_name;

select
    player_name,
    unnest(season_stats) as season_stats
from players
where current_season = 2001
and player_name = 'Michael Jordan';

with unnested as (
    select
        player_name,
        season_stats as full_season_stats,
        unnest(season_stats)::season_stats as season_stats
    from players
    where current_season = 2001
--         and player_name = 'Michael Jordan'
    )
select player_name,
       full_season_stats,
       (season_stats::season_stats).*
FROM unnested;

select * from players
where player_name = 'Michael Jordan';

select * from players
where current_season = 2001;

select
    player_name,
    current_season,
    (season_stats[1]::season_stats).pts as first_season,
    season_stats[1] as first_season_stats,
    (season_stats[cardinality(season_stats)]::season_stats).pts as latest_season,
    season_stats[cardinality(season_stats)] as latest_season_stats,
    (season_stats[cardinality(season_stats)]::season_stats).pts /
    case
        when (season_stats[1]::season_stats).pts = 0
        then 1
        else ((season_stats[1]::season_stats).pts)
    end improved_ratio
from players
where current_season = 2001
order by improved_ratio desc;

select
    player_name,
    current_season,
    season_stats,
    (season_stats[1]::season_stats).pts as first_season,
    season_stats[1] as first_season_stats,
    (season_stats[cardinality(season_stats)]::season_stats).pts as latest_season,
    season_stats[cardinality(season_stats)] as latest_season_stats,
    (season_stats[cardinality(season_stats)]::season_stats).pts /
    case
        when (season_stats[1]::season_stats).pts = 0
            then 1
        else ((season_stats[1]::season_stats).pts)
        end improved_ratio
from players
order by player_name, current_season;

select * from players
where current_season = 2001
and player_name = 'Michael Jordan';

-- day 2
-- drop table players_scd;
--
-- create table players_scd
-- (
--     player_name   text,
--     scoring_class scoring_class,
--     is_active boolean,
--     start_season integer,
--     end_season integer,
--     current_season integer,
--     primary key (player_name, start_season, end_season, current_season)
-- );
--
-- insert into players_scd
-- with with_previous as(
--     select
--         player_name,
--         current_season,
--         scoring_class,
--         is_active,
--         LAG(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
--         LAG(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
--     from players
--     where current_season <= 2022
-- ),
-- with_indicators as (
--     select
--         *,
--         case
--             when scoring_class <> previous_scoring_class then 1
--             when is_active <> previous_is_active then 1
--             else 0
--         end as change_indicator
--     from with_previous
-- ),
-- with_streaks as (
--     select
--         *,
--         sum(change_indicator)
--             over (partition by player_name order by current_season) as streak_identifier
--     from with_indicators
-- )
-- select
--     player_name,
--     scoring_class,
--     is_active,
--     min(current_season) as start_season,
--     max(current_season) as end_season,
--     2022 as current_season
-- from with_streaks
-- group by player_name, streak_identifier, scoring_class, is_active
-- order by player_name, streak_identifier;

select * from players_scd;

-- create type scd_type as (
--     scoring_class scoring_class,
--     is_active boolean,
--     start_season integer,
--     end_season integer
-- );

with last_season_scd AS (
    select * from players_scd
    where current_season = 2021
    and end_season = 2021
),
     historical_scd AS (
         select
             player_name,
             scoring_class,
             is_active,
             start_season,
             end_season
         from players_scd
         where current_season = 2021
           and end_season < 2021
     ),
    this_season_scd AS (
    select * from players_scd
    where current_season = 2022
),
    unchanged_records AS (
        select
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ls.start_season,
            ts.current_season as end_season
        from this_season_scd ts
        join last_season_scd ls
            on ls.player_name = ts.player_name
        where ts.scoring_class = ls.scoring_class
          and ts.is_active = ls.is_active
    ),
    changed_records AS (
        select
            ts.player_name,
--             ts.scoring_class,
--             ts.is_active,
--             ls.start_season,
--             ts.current_season as end_season,
            unnest(array [
                row(
                    ls.scoring_class,
                    ls.is_active,
                    ls.start_season,
                    ls.end_season
                    )::scd_type,
                row(
                    ts.scoring_class,
                    ts.is_active,
                    ts.current_season,
                    ts.current_season
                    )::scd_type ]) as records
        from this_season_scd ts
         left join last_season_scd ls
              on ls.player_name = ts.player_name
        where (ts.scoring_class <> ls.scoring_class)
           or (ts.is_active = ls.is_active)
           or ls.player_name is null
    ),
    unnested_changed_records AS (
        select
            player_name,
            (records::scd_type).scoring_class,
            (records::scd_type).is_active,
            (records::scd_type).start_season,
            (records::scd_type).end_season
        from changed_records
    ),
    new_records AS (
        select
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ts.current_season as start_season,
            ts.current_season as end_season
        from this_season_scd ts
        left join last_season_scd ls
            on ls.player_name = ts.player_name
        where ls.player_name is null
    )
select union_s.* from (select *, 'historical' as typeh
               from historical_scd
               union all
               select *, 'unchanged' as typeh
               from unchanged_records
               union all
               select *, 'unnested_changed' as typeh
               from unnested_changed_records
               union all
               select *, 'new' as typeh
               from new_records) as union_s
order by union_s.player_name, union_s.start_season;



