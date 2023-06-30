-- create tables in duckdb

CREATE TABLE bike_data AS SELECT * FROM read_csv_auto('data/bike/202*.csv.gz', union_by_name=True, sample_size=-1);
CREATE TABLE central_park_weather AS SELECT * FROM 'data/central_park_weather.csv';
CREATE TABLE daily_citi_bike_trip_counts_and_weather AS SELECT * FROM 'data/daily_citi_bike_trip_counts_and_weather.csv';
CREATE TABLE fhv_bases AS SELECT * FROM read_csv_auto('data/fhv_bases.csv', header=TRUE);
CREATE TABLE fhv_tripdata AS SELECT * FROM 'data/taxi/fhv_tripdata_*.parquet';
CREATE TABLE fhvhv_tripdata AS SELECT * FROM 'data/taxi/fhvhv_tripdata_*.parquet';
CREATE TABLE taxi_yellow AS SELECT * FROM 'data/taxi/yellow_*.parquet';
CREATE TABLE taxi_green AS SELECT * FROM 'data/taxi/green_*.parquet';

