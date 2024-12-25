/********************************
SQL Engine: Postgres
Dates and user ids used are examples.
*********************************/

-- 1) How many users were active on a given day (they made a deposit or withdrawal)
with activity as (
    select
        a.user_id
        ,max(case
            when b.id is not null
                then true
                else false
        end) as fl_active
    from users as a
    left join transactions as b
        on a.user_id = b.user_id
    where
        1 = 1
    and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"
    group by
        a.user_id
)

select
    count(user_id) as qty_active_users
from activity
where
    fl_active = true


-- 2) Identify users haven't made a deposit
with users_deposits as (
    select
        a.user_id
        ,max(case
            when b.id is null
                then true
                else false
        end) as fl_no_deposits
    from users as a
    left join transactions as b
        on  a.user_id = b.user_id
    where
        1 = 1
    and direction = 'in'
    group by
        a.user_id
)

select
    user_id
from users_deposits
where
    fl_no_deposits = true


-- 3) Average deposited amount for Level 2 users in the mx jurisdiction
with user_current_level as (
    select
        a.user_id
        ,first_value(b.level) over (partition by a.user_id order by b.event_timestamp desc) as current_level
    from users as a
    left join user_level as b
        on  a.user_id = b.user_id
    where
        1 = 1
    and jurisdiction = 'mx'
),
    users_level_transactions as (
    select
        a.user_id
        ,b.current_level
        ,to_char(c.event_timestamp, 'YYY-MM-DD') as trn_date
        ,c.amount
    from users as a
    inner join user_current_level as b
        on  a.user_id = b.user_id
        and b.current_level = 2
    left join transactions as c
        on  a.user_id = c.user_id
    where
        1 = 1
    and c.direction = 'in'
)

select
    user_id
    ,round(avg(amount), 4) as avg_deposit_amount -- all time - consider updating this KPI to moving avg
from users_level_transactions
group by
    user_id


-- 4) Latest user level for each user within each jurisdiction
select
    a.user_id
    ,b.jurisdiction
    ,first_value(b.level) over (partition by a.user_id, b.jurisdiction order by b.event_timestamp desc) as current_level
from users as a
left join user_level as b
    on  a.user_id = b.user_id


-- 5) Identify on a given day which users have made more than 5 deposits historically
 -- I could also count the transaction ids on a given day;

with users_deposits as (
    select
        a.user_id
        ,to_char(b.event_timestamp, 'YYYY-MM-DD') as trn_date
        ,sum(case
            when b.id is not null
                then 1
                else 0
        end) as qty_deposits
    from users as a
    inner join transactions as b
        on  a.user_id = b.user_id
        and direction = 'in'
    where
        1 = 1
    group by
        a.user_id
)

select
    user_id
    ,trn_date
    ,qty_deposits
from users_deposits
where
    1 = 1
and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"
and qty_deposits > 5


-- 6) When was the last time a user made a login

with user_events as (
    select
        a.user_id
        ,b.event_name
        ,b.event_timestamp
    from users as a
    left join events as b
        on  a.user_id  = b.user_id
    where
        1 = 1
    and b.event_name = 'login'
)

select
    user_id
    ,max(event_timestamp) as last_time_login
from user_events
where
    1 = 1
-- and user_id = 'some_user_id'
group by
    user_id


-- 7) How many times a user has made a login between two dates
select
    a.user_id,
    count(*) as qty_logins
from users as a
join events as b
    on a.user_id = b.user_id
where
    1 = 1
and b.event_name = 'login'
and to_char(b.event_timestamp, 'YYY-MM-DD') between '2024-12-24' and '2024-12-25'
group by
    a.user_id


-- 8) Number of unique currencies deposited on a given day
select
    to_char(b.event_timestamp, 'YYYY-MM-DD') as trn_date
    ,count(distinct currency) as unique_qty_currency
from transactions
where
    1 = 1
and direction = 'in'
and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"


-- 9) Number of unique currencies withdrew on a given day
select
    to_char(b.event_timestamp, 'YYYY-MM-DD') as trn_date
    ,count(distinct currency) as unique_qty_currency
from transactions
where
    1 = 1
and direction = 'out'
and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"


-- 10) Number of unique currencies withdrew on a given day
select
    to_char(b.event_timestamp, 'YYYY-MM-DD') as trn_date
    ,count(distinct currency) as unique_qty_currency
from transactions
where
    1 = 1
and direction = 'out'
and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"


-- 11) Total amount deposited of a given currency on a given day
select
    to_char(b.event_timestamp, 'YYYY-MM-DD') as trn_date
    ,currency
    ,sum(amount) as total_amount
from transactions
where
    1 = 1
and currency = "mxn"
and to_char(b.event_timestamp, 'YYYY-MM-DD') = "2024-12-25"