--delete from users_growth_accounting;
 -- INSERT INTO users_growth_accounting
with yesterday as
				( select *
					from users_growth_accounting
					where date = date('2023-01-08') ),
	today as
				( select cast(user_id AS TEXT) as user_id,
						date_trunc('day', event_time::timestamp) as today_date,
						count(1)
					from events
					where date_trunc('day', event_time::timestamp) = date('2023-01-09')
									and user_id is not null
					group by user_id,
						date_trunc('day', event_time::timestamp) )
select coalesce (t.user_id,
																		y.user_id) as user_id,
	coalesce (y.first_active_date,
												t.today_date) as first_active_date,
	coalesce (t.today_date,
												y.last_active_date) as last_active_date,
	case
					when y.user_id is null then 'New'
					when y.last_active_date = t.today_date - Interval '1 day' then 'Retained'
					when y.last_active_date < t.today_date - Interval '1 day' then 'Resurrected'
					when t.today_date is null
										and y.last_active_date = y.date then 'Churned'
					else 'Stale'
	end as daily_active_state,
	case
					when y.user_id is null then 'New'
					when y.last_active_date < t.today_date - Interval '7 day' then 'Resurrected'
					when t.today_date is null
										and y.last_active_date = y.date - interval '7 day' then 'Churned'
					when coalesce(t.today_date, y.last_active_date) + interval '7 day' >= y.date then 'Retained'
					else 'Stale'
	end as weekly_active_state,
	coalesce (y.dates_active, array[]::date[]) || case
																																																			when t.user_id is not null then array[t.today_date]
																																																			else array[]::date[]
																																															end as date_list,
	coalesce (t.today_date,
												y.date + interval '1 day') as date
from today t
full outer join yesterday y on t.user_id = y.user_id;

--SELECT * FROM users_growth_accounting order by user_ID, DATE;
