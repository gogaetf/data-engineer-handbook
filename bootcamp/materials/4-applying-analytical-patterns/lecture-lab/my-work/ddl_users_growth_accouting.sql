create table users_growth_accounting (
	user_id text,
	first_active_date date,
	last_active_date date,
	daily_active_state text,
	weekly_active_state text,
	dates_active date[],
	date date,
	primary key (user_id, date)
);