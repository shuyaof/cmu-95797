with cte1 as (
select
    weekday(pickup_datetime) as weekday,
    count(*) as total_trips
from {{ ref('mart__fact_all_taxi_trips') }}
group by weekday
),

cte2 as(
select 
    weekday(pickup_datetime) as weekday,
    count(*) as diff_boro_trips
from {{ ref('mart__fact_all_taxi_trips') }}
join {{ ref('mart__dim_locations') }} on locationid = dolocationid
join {{ ref('mart__dim_locations') }} as pickup_loc on pulocationid = pickup_loc.LocationID
join {{ ref('mart__dim_locations') }} as dropoff_loc on dolocationid = dropoff_loc.LocationID
where pickup_loc.borough <> dropoff_loc.borough
group by weekday
)

select 
    cte1.weekday as weekday,
    cte1.total_trips as total_trips,
    cte2.diff_boro_trips as diff_boro_trips,
    100 * cte2.diff_boro_trips / cte1.total_trips as percentage_diff_boro_trips
from cte1 join cte2 on cte1.weekday = cte2.weekday 