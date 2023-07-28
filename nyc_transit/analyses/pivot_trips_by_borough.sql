-- creates a new table where column headers come from row values

-- PIVOT {{ ref('mart__fact_trips_by_borough') }}
-- using count(pulocationid)
-- group by borough

select  
        {{ dbt_utils.pivot(
        'borough',
        dbt_utils.get_column_values(ref('mart__fact_trips_by_borough'), 'borough'),
        agg='sum',
        then_value = 'total_trips',
        ) }}
from {{ ref('mart__fact_trips_by_borough') }}


