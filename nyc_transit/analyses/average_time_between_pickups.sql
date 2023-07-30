-- Find the average time between taxi pick ups per zone
-- Use lead or lag to find the next trip per zone for each record
-- then find the time difference between the pick up time for the current record and the next
-- then use this result to calculate the average time between pick ups per zone.

-- depends_on: {{ ref('mart__fact_all_taxi_trips') }}
{% set zones = dbt_utils.get_column_values(
    table=ref('mart__dim_locations'),
    column='Zone') %}

{% for zone in zones %}
    {% set zone = zone.replace("'", "''") %} --change single quote to double quote to avoid the error

    SELECT 
        zone,
        AVG(difference) AS average_time_between_pickups
    FROM
    (
        SELECT 
            zone,datediff('second', t.pickup_datetime, lead(t.pickup_datetime) OVER (ORDER BY t.pickup_datetime)) AS difference
        FROM {{ ref('mart__fact_all_taxi_trips') }} t
        JOIN {{ ref('mart__dim_locations') }} l 
        ON l.locationid = t.pulocationid
        WHERE l.zone = '{{ zone }}'
    ) AS time_diffs
    group by 1;
{% endfor %}
 