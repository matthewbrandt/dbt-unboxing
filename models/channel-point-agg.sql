select
    title,
    count(event_id) as nr_of_redeems,
    sum(cost) as points_spent,
    count(distinct redeemed_user_login) as unique_users_redeemed
from {{ ref('channel-point-redeem') }}
where status = 'fulfilled'
group by 1
order by 1