INSERT INTO TABLE zone_temporal_profiles PARTITION(year, month)
SELECT
    zone_id,
    mode,
    day_of_week,
    hour_of_day,
    SUM(population_in) as population_in,
    SUM(population_out) as population_out,
    year,
    month
FROM (
    SELECT
        origin_location_id as zone_id,
        'subway' as mode,
        year,
        month,
        dayofweek(transit_timestamp) as day_of_week,
        hour(transit_timestamp) as hour_of_day,
        0 as population_in,
        CAST(SUM(estimated_average_ridership) AS BIGINT) as population_out
    FROM fact_ridership
    WHERE origin_location_id IS NOT NULL
    GROUP BY 
        origin_location_id,
        year,
        month,
        dayofweek(transit_timestamp),
        hour(transit_timestamp)
    
    UNION ALL
    
    SELECT
        destination_location_id as zone_id,
        'subway' as mode,
        year,
        month,
        dayofweek(transit_timestamp) as day_of_week,
        hour(transit_timestamp) as hour_of_day,
        CAST(SUM(estimated_average_ridership) AS BIGINT) as population_in,
        0 as population_out
    FROM fact_ridership
    WHERE destination_location_id IS NOT NULL
    GROUP BY 
        destination_location_id,
        year,
        month,
        dayofweek(transit_timestamp),
        hour(transit_timestamp)
) subway_data
GROUP BY 
    zone_id, mode, day_of_week, hour_of_day, year, month;


INSERT INTO TABLE zone_temporal_profiles PARTITION(year, month)
SELECT
    zone_id,
    mode,
    day_of_week,
    hour_of_day,
    SUM(population_in) as population_in,
    SUM(population_out) as population_out,
    year,
    month
FROM (
    SELECT
        start_location_id as zone_id,
        'yellow_taxi' as mode,
        yt.year,
        yt.month,
        dayofweek(pickup_ts) as day_of_week,
        hour(pickup_ts) as hour_of_day,
        0 as population_in,
        COUNT(*) as population_out
    FROM yellow_taxi yt
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = yt.year 
        AND mvw.month = yt.month
        AND mvw.week_of_year = weekofyear(yt.pickup_ts)
    WHERE start_location_id IS NOT NULL
    GROUP BY 
        start_location_id,
        yt.year,
        yt.month,
        dayofweek(pickup_ts),
        hour(pickup_ts)
    
    UNION ALL
    
    SELECT
        end_location_id as zone_id,
        'yellow_taxi' as mode,
        yt.year,
        yt.month,
        dayofweek(dropoff_ts) as day_of_week,
        hour(dropoff_ts) as hour_of_day,
        COUNT(*) as population_in,
        0 as population_out
    FROM yellow_taxi yt
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = yt.year 
        AND mvw.month = yt.month
        AND mvw.week_of_year = weekofyear(yt.dropoff_ts)
    WHERE end_location_id IS NOT NULL
    GROUP BY 
        end_location_id,
        yt.year,
        yt.month,
        dayofweek(dropoff_ts),
        hour(dropoff_ts)
) yellow_data
GROUP BY 
    zone_id, mode, day_of_week, hour_of_day, year, month;


INSERT INTO TABLE zone_temporal_profiles PARTITION(year, month)
SELECT
    zone_id,
    mode,
    day_of_week,
    hour_of_day,
    SUM(population_in) as population_in,
    SUM(population_out) as population_out,
    year,
    month
FROM (
    SELECT
        start_location_id as zone_id,
        'green_taxi' as mode,
        gt.year,
        gt.month,
        dayofweek(pickup_ts) as day_of_week,
        hour(pickup_ts) as hour_of_day,
        0 as population_in,
        COUNT(*) as population_out
    FROM green_taxi gt
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = gt.year 
        AND mvw.month = gt.month
        AND mvw.week_of_year = weekofyear(gt.pickup_ts)
    WHERE start_location_id IS NOT NULL
    GROUP BY 
        start_location_id,
        gt.year,
        gt.month,
        dayofweek(pickup_ts),
        hour(pickup_ts)
    
    UNION ALL
    
    SELECT
        end_location_id as zone_id,
        'green_taxi' as mode,
        gt.year,
        gt.month,
        dayofweek(dropoff_ts) as day_of_week,
        hour(dropoff_ts) as hour_of_day,
        COUNT(*) as population_in,
        0 as population_out
    FROM green_taxi gt
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = gt.year 
        AND mvw.month = gt.month
        AND mvw.week_of_year = weekofyear(gt.dropoff_ts)
    WHERE end_location_id IS NOT NULL
    GROUP BY 
        end_location_id,
        gt.year,
        gt.month,
        dayofweek(dropoff_ts),
        hour(dropoff_ts)
) green_data
GROUP BY 
    zone_id, mode, day_of_week, hour_of_day, year, month;


INSERT INTO TABLE zone_temporal_profiles PARTITION(year, month)
SELECT
    zone_id,
    mode,
    day_of_week,
    hour_of_day,
    SUM(population_in) as population_in,
    SUM(population_out) as population_out,
    year,
    month
FROM (
    SELECT
        start_location_id as zone_id,
        'fhvhv' as mode,
        ft.year,
        ft.month,
        dayofweek(pickup_ts) as day_of_week,
        hour(pickup_ts) as hour_of_day,
        0 as population_in,
        COUNT(*) as population_out
    FROM fhvhv_taxi ft
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = ft.year 
        AND mvw.month = ft.month
        AND mvw.week_of_year = weekofyear(ft.pickup_ts)
    WHERE start_location_id IS NOT NULL
    GROUP BY 
        start_location_id,
        ft.year,
        ft.month,
        dayofweek(pickup_ts),
        hour(pickup_ts)
    
    UNION ALL
    
    SELECT
        end_location_id as zone_id,
        'fhvhv' as mode,
        ft.year,
        ft.month,
        dayofweek(dropoff_ts) as day_of_week,
        hour(dropoff_ts) as hour_of_day,
        COUNT(*) as population_in,
        0 as population_out
    FROM fhvhv_taxi ft
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = ft.year 
        AND mvw.month = ft.month
        AND mvw.week_of_year = weekofyear(ft.dropoff_ts)
    WHERE end_location_id IS NOT NULL
    GROUP BY 
        end_location_id,
        ft.year,
        ft.month,
        dayofweek(dropoff_ts),
        hour(dropoff_ts)
) fhvhv_data
GROUP BY 
    zone_id, mode, day_of_week, hour_of_day, year, month;


INSERT INTO TABLE zone_temporal_profiles PARTITION(year, month)
SELECT
    zone_id,
    mode,
    day_of_week,
    hour_of_day,
    SUM(population_in) as population_in,
    SUM(population_out) as population_out,
    year,
    month
FROM (
    SELECT
        start_location_id as zone_id,
        'citibike' as mode,
        ct.year,
        ct.month,
        dayofweek(started_at) as day_of_week,
        hour(started_at) as hour_of_day,
        0 as population_in,
        COUNT(*) as population_out
    FROM citibike_table ct
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = ct.year 
        AND mvw.month = ct.month
        AND mvw.week_of_year = weekofyear(ct.started_at)
    WHERE start_location_id IS NOT NULL
    GROUP BY 
        start_location_id,
        ct.year,
        ct.month,
        dayofweek(started_at),
        hour(started_at)
    
    UNION ALL
    
    SELECT
        end_location_id as zone_id,
        'citibike' as mode,
        ct.year,
        ct.month,
        dayofweek(ended_at) as day_of_week,
        hour(ended_at) as hour_of_day,
        COUNT(*) as population_in,
        0 as population_out
    FROM citibike_table ct
    INNER JOIN mta_valid_weeks mvw
        ON mvw.year = ct.year 
        AND mvw.month = ct.month
        AND mvw.week_of_year = weekofyear(ct.ended_at)
    WHERE end_location_id IS NOT NULL
    GROUP BY 
        end_location_id,
        ct.year,
        ct.month,
        dayofweek(ended_at),
        hour(ended_at)
) citibike_data
GROUP BY 
    zone_id, mode, day_of_week, hour_of_day, year, month;
