SELECT *
FROM users_growth_accounting
order by user_ID, DATE;


select date, daily_active_state,
             count(1)
from users_growth_accounting
group by date, daily_active_state
order by date, daily_active_state;


SELECT *
from users_growth_accounting
where first_active_date = date('2023-01-01');


SELECT extract(dow
               from date) as dow, date - first_active_date as days_since_first_active,
                                         count( case
                                                    when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
                                                end ) as num_active,
                                         count(1) as total,
                                         cast(count( case
                                                         when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
                                                     end ) as real) / count(1) as percent_active
from users_growth_accounting
where first_active_date = date('2023-01-01')
GROUP BY extract(dow
                 from date), date, date - first_active_date
order BY date;
