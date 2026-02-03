DROP TABLE IF EXISTS green_taxi;

CREATE EXTERNAL TABLE green_taxi (
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    start_location_id INT,
    end_location_id INT,
    trip_distance DOUBLE,
    passenger_count INT,
    taxi_type STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
LOCATION '/user/ys4285_nyu_edu/taxi/partitioned/green/';

MSCK REPAIR TABLE green_taxi;

SHOW PARTITIONS green_taxi;

SELECT year, month, COUNT(*) as record_count
FROM green_taxi
GROUP BY year, month
ORDER BY year, month;

SELECT * FROM green_taxi LIMIT 10;