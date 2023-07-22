select 
    count(*) as trips,
from {{ ref('mart__fact_all_taxi_trips') }}
where dolocationid in (select locationid
from {{ ref('taxi+_zone_lookup') }}
where service_zone in ('EWR','Airports')
)