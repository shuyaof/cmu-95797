.echo on

-- print a list of table names
SHOW TABLES;

ALTER TABLE bike_data ADD filename VARCHAR;
ALTER TABLE central_park_weather ADD filename VARCHAR;
ALTER TABLE daily_citi_bike_trip_counts_and_weather ADD filename VARCHAR;
ALTER TABLE fhv_bases ADD filename VARCHAR;
ALTER TABLE fhv_tripdata ADD filename VARCHAR;
ALTER TABLE fhvhv_tripdata ADD filename VARCHAR;
ALTER TABLE taxi_yellow ADD filename VARCHAR;
ALTER TABLE taxi_green ADD filename VARCHAR;

--print a list of schemas
DESCRIBE bike_data;
DESCRIBE central_park_weather;
DESCRIBE daily_citi_bike_trip_counts_and_weather;
DESCRIBE fhv_bases;
DESCRIBE fhv_tripdata;
DESCRIBE fhvhv_tripdata;
DESCRIBE taxi_yellow;
DESCRIBE taxi_green;


