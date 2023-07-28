select 
        borough,
        count(t.pulocationid) as total_trips,
from {{ ref('mart__dim_locations') }} tz
join {{ ref('mart__fact_all_taxi_trips') }} t on t.pulocationid = tz.locationid
group by borough

