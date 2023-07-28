-- individual fare avg fare for the zone
-- from yellow taxi trip data

select fare_amount, 
       avg(fare_amount) over (partition by Zone) as avg_zone_fare,
       avg(fare_amount) over (partition by Borough) as avg_borough_fare,
       avg(fare_amount) over () as overall_avg_fare
from {{ ref('stg__yellow_tripdata')}} y 
join {{ ref('mart__dim_locations')}} l on l.locationid = y.pulocationid
