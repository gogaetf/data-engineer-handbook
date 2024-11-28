select type, count(1)
from vertices
group by type ;

select
    v.properties->>'player_name',
    max(e.properties->>'pts') as max_pts
from vertices v
join edges e
    on e.subject_identifier = v.identifier
    and e.subject_type = v.type
group by 1
order by 2 desc;

select
    v.properties->>'player_name',
    max(cast(e.properties->>'pts' as integer)) as max_pts
from vertices v
         join edges e
              on e.subject_identifier = v.identifier
                  and e.subject_type = v.type
group by 1
order by 2 desc;

select * from vertices where type = 'player'::vertex_type and identifier = '203504';
select * from edges where object_type = 'player'::vertex_type and subject_identifier = '203504';

select * from edges
where subject_type = 'player'::vertex_type and object_type = 'player'::vertex_type;

select
    v.properties->>'player_name',
    v.identifier vertex_id,
    e.object_identifier as object_id,
    e.subject_type as subject_type,
    e.object_type as object_type,
    e.edge_type,
    cast(v.properties->>'number_of_games' as real) /
    case
        when cast(v.properties->>'total_points' as real) = 0
        then 1
        else cast(v.properties->>'total_points' as real)
    end as pts_per_game,
    e.properties->>'subject_points' as subject_points,
    e.properties->>'num_games' as num_games
from vertices v
join edges e
    on v.identifier = e.subject_identifier
        and v.type = e.subject_type
where e.object_type = 'player'::vertex_type;

select
    *
from vertices v
join edges e
    on v.identifier = e.subject_identifier
        and v.type = e.subject_type
where e.object_type = 'player'::vertex_type;

select
    v.*,
    e.*
from vertices v
         join edges e
              on v.identifier = e.subject_identifier
                  and v.type = e.subject_type
where e.object_type = 'player'::vertex_type;

-- player vs player edge
-- insert into edges
with deduped as (
    select *, row_number() over (partition by player_id, game_id) as row_num
    from game_details
),
filtered as (
    select * from deduped
    where row_num = 1
),
aggregated as (
    select
        f1.player_id as subject_player_id,
        max(f1.player_name) as subject_player_name, --player can have more than one name in case of change
        f2.player_id as object_player_id,
        max(f2.player_name) as object_player_name, --player can have more than one name in case of change
        case
            when f1.team_abbreviation = f2.team_abbreviation
            then 'shares_team'::edge_type
            else 'plays_against'::edge_type
        end as edge_type,
        count(1) as num_games,
        sum(f1.pts) as subject_points,
        sum(f2.pts) as object_points
    from filtered f1
    join filtered f2
        on f1.game_id = f2.game_id
        and f1.player_name <> f2.player_name
    where f1.player_id > f2.player_id -- avoid duplicates, it doesn't matter who is the left or right player
    group by
        f1.player_id,
        f2.player_id,
        case
            when f1.team_abbreviation = f2.team_abbreviation
                then 'shares_team'::edge_type
            else 'plays_against'::edge_type
            end
)
select
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    ) as properties
from aggregated;


select * from game_details;

-- insert into edges
-- player vs game edge
with deduped as (
    select *, row_number() over (partition by player_id, game_id) as row_num
    from game_details
)
select
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) as properties
from deduped
where row_num = 1;

select * from edges;
select * from edges where object_type = 'game'::vertex_type ;




