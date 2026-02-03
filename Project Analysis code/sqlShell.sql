
################################ SQL command ############################################################


################# Combine Taxi table ################# 
CREATE TABLE taxi_table (
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    start_location_id INT,
    end_location_id INT,
    trip_distance DOUBLE,
    passenger_count INT,
    taxi_type STRING
)
PARTITIONED BY (year INT, month INT)
STORED as ORC;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE taxi_table
PARTITION (year, month)
SELECT * FROM green_taxi
UNION ALL
SELECT * FROM yellow_taxi
UNION ALL
SELECT * FROM fhvhv_taxi;
################################################### 









################################################### 
-- # Create MTA Ridership Table

CREATE EXTERNAL TABLE ridership_mta_table (
  transit_timestamp             TIMESTAMP,
  origin_station_complex_name   STRING,
  origin_latitude               DOUBLE,
  origin_longitude              DOUBLE,
  destination_station_complex_name STRING,
  destination_latitude          DOUBLE,
  destination_longitude         DOUBLE,
  estimated_average_ridership   DOUBLE,
  origin_location_id            INT,
  destination_location_id       INT
)
PARTITIONED BY (year INT, month INT)
STORED as ORC
LOCATION '/user/tl4549_nyu_edu/mta/fact_ridership';

MSCK REPAIR TABLE ridership_mta_table;
################################################### 








################################################### 
-- # Create the first analysis idea: Compute the total transportation volume for each zone,
-- # and calculate the proportion of each transportation mode

create table zone_traffic_analysis_daily
stored as orc
as with
taxi_valid_days as (
    select count(distinct cast(pickup_ts as date)) as days from taxi_table
),
subway_valid_days as (
    select count(distinct cast(transit_timestamp as date)) as days from ridership_mta_table
),
citibike_valid_days as (
    select count(distinct cast(started_at as date)) as days from citibike_table
),
taxi_summary as (
    select zone_id, sum(passengers) as taxi_passengers
    from (
        select start_location_id as zone_id, sum(passenger_count) as passengers
        from taxi_table
        group by start_location_id
        union all
        select end_location_id as zone_id, sum(passenger_count) as passengers
        from taxi_table
        group by end_location_id
    ) taxi_data
    group by zone_id
),
subway_summary as (
    select zone_id, sum(passengers) as subway_ridership
    from (
        select origin_location_id as zone_id, sum(estimated_average_ridership) as passengers
        from ridership_mta_table
        group by origin_location_id
        union all 
        select destination_location_id as zone_id, sum(estimated_average_ridership) as passengers
        from ridership_mta_table
        group by destination_location_id
    ) subway_data
    group by zone_id
),
citibike_summary as (
    select zone_id, sum(passengers) as citibike_passengers
    from (
        select start_location_id as zone_id, sum(passenger_count) as passengers
        from citibike_table
        group by start_location_id
        union all
        select end_location_id as zone_id, sum(passenger_count) as passengers
        from citibike_table
        group by end_location_id
    ) citibike_data
    group by zone_id
),
zone_totals as (
    select COALESCE(t.zone_id, s.zone_id, c.zone_id) as zone_id,
    COALESCE(t.taxi_passengers, 0) as taxi_passengers,
    COALESCE(s.subway_ridership, 0) as subway_ridership,
    COALESCE(c.citibike_passengers, 0) as citibike_passengers
    from taxi_summary t
    full outer join subway_summary s
        on t.zone_id = s.zone_id
    full outer join citibike_summary c
        on COALESCE(t.zone_id, s.zone_id) = c.zone_id
)
select
    z.zone_id,
    (z.taxi_passengers + z.subway_ridership + z.citibike_passengers) as total_passengers,
    z.taxi_passengers,
    z.subway_ridership,
    z.citibike_passengers,
    tv.days as taxi_valid_days,
    sv.days as subway_valid_days,
    cv.days as citibike_valid_days,
    
    case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end as taxi_daily_passengers,
    case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end as subway_daily_passengers,
    case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end as citibike_daily_passengers,

    ( case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
      + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
      + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
    ) as total_daily_passengers,

    case 
        when (
            case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
          + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
          + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
        ) > 0
        then ROUND(
            (case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end) * 100.0 /
            (
                case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
              + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
              + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
            ),
            2
        )
        else 0
    end as taxi_percentage_daily,

    case 
        when (
            case when tv.days > 0 then z.taxi_passengers * 1.0  / tv.days else 0.0 end
          + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
          + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
        ) > 0
        then ROUND(
            (case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end) * 100.0 /
            (
                case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
              + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
              + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
            ),
            2
        )
        else 0
    end as subway_percentage_daily,

    case 
        when (
            case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
          + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
          + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
        ) > 0
        then ROUND(
            (case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end) * 100.0 /
            (
                case when tv.days > 0 then z.taxi_passengers * 1.0 / tv.days else 0.0 end
              + case when sv.days > 0 then z.subway_ridership * 1.0 / sv.days else 0.0 end
              + case when cv.days > 0 then z.citibike_passengers * 1.0 / cv.days else 0.0 end
            ),
            2
        )
        else 0
    end as citibike_percentage_daily

from zone_totals z
cross join taxi_valid_days tv
cross join subway_valid_days sv
cross join citibike_valid_days cv
where (z.taxi_passengers + z.subway_ridership + z.citibike_passengers) > 0;
################################################### 







################################################### 
-- # Create the second analysis idea: Evaluate the real impact of events on transportation demand
-- # Although this approach is the most compreshensive, it is also very slow due to large table scans.

create table event_impact_analysis
stored as orc
as with
event_window as (
    select
        event_id,
        event_name,
        event_type,
        start_location_id as zone_id,
        start_time,
        end_time,
        start_time - INTERVAL 90 MINUTES as pre_start_time,
        end_time + INTERVAL 90 MINUTES as post_end_time
    from events_table
),
event_actual as (
    select 
        e.event_id,
        e.event_name,
        e.event_type,
        e.zone_id,
        sum(
            case 
                when t.dropoff_ts >= e.pre_start_time
                and t.dropoff_ts < e.start_time and t.end_location_id = e.zone_id
                then t.passenger_count else 0
            end
        ) as taxi_inbound_pre,
        sum(
            case 
                when t.pickup_ts >= e.end_time 
                and t.pickup_ts < e.post_end_time and t.start_location_id = e.zone_id
                then t.passenger_count else 0
            end
        ) as taxi_outbound_post,
        sum(
            case
                when s.transit_timestamp >= e.pre_start_time
                and s.transit_timestamp < e.start_time and s.destination_location_id = e.zone_id
                then s.estimated_average_ridership else 0
            end
        ) as subway_inbound_pre,
        sum(
            case
                when s.transit_timestamp >= e.end_time
                and s.transit_timestamp < e.post_end_time and s.origin_location_id = e.zone_id
                then s.estimated_average_ridership else 0
            end
        ) as subway_outbound_post,
        sum(
            case 
                when c.ended_at >= e.pre_start_time
                and c.ended_at < e.start_time and c.end_location_id = e.zone_id
                then c.passenger_count else 0
            end
        ) as citibike_inbound_pre,
        sum(
            case 
                when c.started_at >= e.end_time
                and c.started_at < e.post_end_time and c.start_location_id = e.zone_id
                then c.passenger_count else 0
            end
        ) as citibike_outbound_post

    from event_window e
    left join taxi_table t 
        on (
            (t.dropoff_ts between e.pre_start_time and e.start_time and t.end_location_id = e.zone_id)
            or
            (t.pickup_ts between e.end_time and e.post_end_time and t.start_location_id = e.zone_id)
        )
    left join ridership_mta_table s
        on (
            (s.transit_timestamp between e.pre_start_time and e.start_time and s.destination_location_id = e.zone_id)
            or
            (s.transit_timestamp between e.end_time and e.post_end_time and s.origin_location_id = e.zone_id)
        )
    left join citibike_table c
        on (
            (c.ended_at between e.pre_start_time and e.start_time and c.end_location_id = e.zone_id)
            or
            (c.started_at between e.end_time and e.post_end_time and c.start_location_id = e.zone_id)
        )   
    group by e.event_id, e.event_name, e.event_type, e.zone_id
),
event_exclude_dates as (
    select distinct
        start_location_id as zone_id,
        DATE(start_time) as exclude_date
    from events_table
    union
    select distinct
        start_location_id as zone_id,
        DATE(end_time) as exclude_date
    from events_table
),
taxi_daily_baseline as (
    select
        e.event_id,
        DATE(t.dropoff_ts) as baseline_date,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.start_time) % 86400 < to_unix_timestamp(e.pre_start_time) % 86400 then 
                    (
                        to_unix_timestamp(t.dropoff_ts) % 86400 >= to_unix_timestamp(e.pre_start_time) % 86400
                        or to_unix_timestamp(t.dropoff_ts) % 86400 <= to_unix_timestamp(e.start_time) % 86400
                    )
                    else (
                        to_unix_timestamp(t.dropoff_ts) % 86400 between to_unix_timestamp(e.pre_start_time) % 86400 and to_unix_timestamp(e.start_time) % 86400
                    )
                    end
                )
                then t.passenger_count else 0 end
        ) as baseline_inbound,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.post_end_time) % 86400 < to_unix_timestamp(e.end_time) % 86400 then 
                    (
                        to_unix_timestamp(t.pickup_ts) % 86400 >= to_unix_timestamp(e.end_time) % 86400
                        or to_unix_timestamp(t.pickup_ts) % 86400 <= to_unix_timestamp(e.post_end_time) % 86400
                    )
                    else (
                        to_unix_timestamp(t.pickup_ts) % 86400 between to_unix_timestamp(e.end_time) % 86400 and to_unix_timestamp(e.post_end_time) % 86400
                    )
                    end
                )
                then t.passenger_count else 0 end
         ) as baseline_outbound
    
    from event_window e
    inner join taxi_table t on (t.end_location_id = e.zone_id or t.start_location_id = e.zone_id) 
    left join event_exclude_dates ex on ex.zone_id = e.zone_id and ex.exclude_date = DATE(t.dropoff_ts)
    left join event_exclude_dates ex2 on ex2.zone_id = e.zone_id and ex2.exclude_date = DATE(t.pickup_ts)
    where ex.exclude_date is null and ex2.exclude_date is null 
    group by e.event_id, DATE(t.dropoff_ts)
),
subway_daily_baseline as (
    select
        e.event_id,
        DATE(s.transit_timestamp) as baseline_date,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.start_time) % 86400 < to_unix_timestamp(e.pre_start_time) % 86400 then 
                    (
                        to_unix_timestamp(s.transit_timestamp) % 86400 >= to_unix_timestamp(e.pre_start_time) % 86400
                        or to_unix_timestamp(s.transit_timestamp) % 86400 <= to_unix_timestamp(e.start_time) % 86400
                    )
                    else (
                        to_unix_timestamp(s.transit_timestamp) % 86400 between to_unix_timestamp(e.pre_start_time) % 86400 and to_unix_timestamp(e.start_time) % 86400
                    )
                    end
                )
                then s.estimated_average_ridership else 0 end
        ) as baseline_inbound,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.post_end_time) % 86400 < to_unix_timestamp(e.end_time) % 86400 then 
                    (
                        to_unix_timestamp(s.transit_timestamp) % 86400 >= to_unix_timestamp(e.end_time) % 86400
                        or to_unix_timestamp(s.transit_timestamp) % 86400 <= to_unix_timestamp(e.post_end_time) % 86400
                    )
                    else (
                        to_unix_timestamp(s.transit_timestamp) % 86400 between to_unix_timestamp(e.end_time) % 86400 and to_unix_timestamp(e.post_end_time) % 86400
                    )
                    end
                )
                then s.estimated_average_ridership else 0 end
         ) as baseline_outbound 
    from event_window e
    inner join ridership_mta_table s on (s.destination_location_id = e.zone_id or s.origin_location_id = e.zone_id) 
    left join event_exclude_dates ex on ex.zone_id = e.zone_id and ex.exclude_date = DATE(s.transit_timestamp)
    where ex.exclude_date is null 
    group by e.event_id, DATE(s.transit_timestamp)
),
citibike_daily_baseline as (
    select
        e.event_id,
        DATE(c.ended_at) as baseline_date,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.start_time) % 86400 < to_unix_timestamp(e.pre_start_time) % 86400 then 
                    (
                        to_unix_timestamp(c.ended_at) % 86400 >= to_unix_timestamp(e.pre_start_time) % 86400
                        or to_unix_timestamp(c.ended_at) % 86400 <= to_unix_timestamp(e.start_time) % 86400
                    )
                    else (
                        to_unix_timestamp(c.ended_at) % 86400 between to_unix_timestamp(e.pre_start_time) % 86400 and to_unix_timestamp(e.start_time) % 86400
                    )
                    end
                )
                then c.passenger_count else 0 end
        ) as baseline_inbound,
        sum(
            case 
                when (
                    case when to_unix_timestamp(e.post_end_time) % 86400 < to_unix_timestamp(e.end_time) % 86400 then 
                    (
                        to_unix_timestamp(c.started_at) % 86400 >= to_unix_timestamp(e.end_time) % 86400
                        or to_unix_timestamp(c.started_at) % 86400 <= to_unix_timestamp(e.post_end_time) % 86400
                    )
                    else (
                        to_unix_timestamp(c.started_at) % 86400 between to_unix_timestamp(e.end_time) % 86400 and to_unix_timestamp(e.post_end_time) % 86400
                    )
                    end
                )
                then c.passenger_count else 0 end
         ) as baseline_outbound 
    from event_window e
    inner join citibike_table c on (c.end_location_id = e.zone_id or c.start_location_id = e.zone_id) 
    left join event_exclude_dates ex on ex.zone_id = e.zone_id and ex.exclude_date = DATE(c.ended_at)
    left join event_exclude_dates ex2 on ex2.zone_id = e.zone_id and ex2.exclude_date = DATE(c.started_at)
    where ex.exclude_date is null and ex2.exclude_date is null
    group by e.event_id, DATE(c.ended_at)
),
taxi_baseline as (
    select
        event_id,
        avg(baseline_inbound) as baseline_taxi_inbound,
        avg(baseline_outbound) as baseline_taxi_outbound
    from taxi_daily_baseline
    group by event_id
),
subway_baseline as (
    select
        event_id,
        avg(baseline_inbound) as baseline_subway_inbound,
        avg(baseline_outbound) as baseline_subway_outbound
    from subway_daily_baseline
    group by event_id
),
citibike_baseline as (
    select
        event_id,
        avg(baseline_inbound) as baseline_citibike_inbound,
        avg(baseline_outbound) as baseline_citibike_outbound
    from citibike_daily_baseline
    group by event_id
)

select a.*, tb.baseline_taxi_inbound, tb.baseline_taxi_outbound,
       sb.baseline_subway_inbound, sb.baseline_subway_outbound,
       cb.baseline_citibike_inbound, cb.baseline_citibike_outbound,
       ROUND(a.taxi_inbound_pre / NULLIF(tb.baseline_taxi_inbound, 0), 2) AS taxi_inbound_ratio,
       ROUND(a.taxi_outbound_post / NULLIF(tb.baseline_taxi_outbound, 0), 2) AS taxi_outbound_ratio,
       ROUND(a.subway_inbound_pre / NULLIF(sb.baseline_subway_inbound, 0), 2) AS subway_inbound_ratio,
       ROUND(a.subway_outbound_post / NULLIF(sb.baseline_subway_outbound, 0), 2) AS subway_outbound_ratio,
       ROUND(a.citibike_inbound_pre / NULLIF(cb.baseline_citibike_inbound, 0), 2) AS citibike_inbound_ratio,
       ROUND(a.citibike_outbound_post / NULLIF(cb.baseline_citibike_outbound, 0), 2) AS citibike_outbound_ratio

from event_actual a
left join taxi_baseline tb on a.event_id = tb.event_id
left join subway_baseline sb on a.event_id = sb.event_id
left join citibike_baseline cb on a.event_id = cb.event_id
order by a.event_id;
################################################### 








################################################### 

-- # Create the third analysis idea: From January to December 2024, 
-- # randomly select a mid-week date for each month, then randomly sample 12 events from those dates.

-- # The main reason for this approach is that full table joins are extremely large and slow.
-- # Even after waiting for over an hour, the query didnt finish.
-- # So we can only use this idea to reduce data volume and improve performance.


WITH subway_sample_dates AS (
    SELECT DISTINCT CAST(transit_timestamp AS DATE) AS sample_date
    FROM ridership_mta_table
    WHERE transit_timestamp >= DATE '2024-01-01'
      AND transit_timestamp <  DATE '2025-01-01'
),
subway_sample_weeks AS (
    SELECT
        year(sample_date)  AS sample_year,
        month(sample_date) AS sample_month,
        MIN(sample_date)   AS week_start_date
    FROM subway_sample_dates
    GROUP BY year(sample_date), month(sample_date)
),
mid_sample_dates AS (
    SELECT
        sample_year,
        sample_month,
        date_add('day', 3, week_start_date) AS mid_sample_date
    FROM subway_sample_weeks
),
events_on_mid_sample_dates AS (
    SELECT
        e.*,
        CAST(e.start_time AS DATE) AS event_date
    FROM events_table e
    JOIN mid_sample_dates m
      ON CAST(e.start_time AS DATE) = m.mid_sample_date
    WHERE e.start_time >= TIMESTAMP '2024-01-01 00:00:00'
      AND e.start_time <  TIMESTAMP '2025-01-01 00:00:00'
      AND date_diff('hour', e.start_time, e.end_time) <= 5
)
SELECT
    event_id,
    event_name,
    event_type,
    start_location_id,
    end_location_id,
    start_time,
    end_time
FROM events_on_mid_sample_dates
ORDER BY random()
LIMIT 12;


-- event_id |                                event_name                                |  event_type   | start_location_id | end_location_id |       start_time        |        end_time         
-- ----------+--------------------------------------------------------------------------+---------------+-------------------+-----------------+-------------------------+-------------------------
--  810920   | 10-10-24 A Day in the Life of the Hudson and Harbor at Coney Island Pier | Special Event |                55 |              55 | 2024-10-10 10:00:00.000 | 2024-10-10 13:00:00.000 
--  776844   | summer series                                                            | Special Event |               240 |             240 | 2024-07-11 17:00:00.000 | 2024-07-11 20:00:00.000 
--  806389   | Universal Kids Soccer                                                    | Special Event |                43 |              43 | 2024-10-10 09:00:00.000 | 2024-10-10 12:00:00.000 
--  797622   | Celebration                                                              | Special Event |                43 |              43 | 2024-08-08 17:00:00.000 | 2024-08-08 19:00:00.000 
--  810977   | Soccer - Non Regulation                                                  | Sport - Youth |                59 |              59 | 2024-10-10 15:00:00.000 | 2024-10-10 17:30:00.000 
--  751766   | Soccer - Non Regulation                                                  | Sport - Youth |                 8 |               8 | 2024-07-11 16:00:00.000 | 2024-07-11 18:00:00.000 
--  793006   | Soccer -Regulation                                                       | Sport - Youth |               195 |             195 | 2024-11-07 16:00:00.000 | 2024-11-07 20:00:00.000 
--  793055   | Soccer - Non Regulation                                                  | Sport - Youth |                16 |              16 | 2024-11-07 15:00:00.000 | 2024-11-07 18:00:00.000 
--  751570   | Lacrosse                                                                 | Sport - Youth |               195 |             195 | 2024-02-08 15:00:00.000 | 2024-02-08 17:30:00.000 
--  751172   | Softball - Adults                                                        | Sport - Adult |                43 |              43 | 2024-07-11 18:00:00.000 | 2024-07-11 20:00:00.000 
--  757989   | Celebration                                                              | Special Event |                43 |              43 | 2024-08-08 12:00:00.000 | 2024-08-08 14:00:00.000 
--  761847   | Soccer - Non Regulation                                                  | Sport - Youth |                93 |              93 | 2024-08-08 13:30:00.000 | 2024-08-08 18:00:00.000 


CREATE TABLE sampled_events_taxi AS
WITH event_window AS (
    SELECT
        event_id,
        event_name,
        event_type,
        start_location_id AS zone_id,
        start_time,
        end_time,
        start_time - INTERVAL '90' MINUTE AS pre_start_time,
        end_time   + INTERVAL '90' MINUTE AS post_end_time
    FROM sampled_events_12
),
baseline_days AS (
    SELECT 
        event_id,
        event_name,
        event_type,
        zone_id,
        start_time,
        end_time,
        pre_start_time,
        post_end_time,
        (start_time - INTERVAL '1' DAY) - INTERVAL '90' MINUTE AS day_pre_start_1,
        (start_time - INTERVAL '1' DAY) AS day_pre_end_1,
        (start_time - INTERVAL '2' DAY) - INTERVAL '90' MINUTE AS day_pre_start_2,
        (start_time - INTERVAL '2' DAY) AS day_pre_end_2,
        (start_time - INTERVAL '3' DAY) - INTERVAL '90' MINUTE AS day_pre_start_3,
        (start_time - INTERVAL '3' DAY) AS day_pre_end_3,
        (end_time + INTERVAL '1' DAY) AS day_post_start_1,
        (end_time + INTERVAL '1' DAY) + INTERVAL '90' MINUTE AS day_post_end_1,
        (end_time + INTERVAL '2' DAY) AS day_post_start_2,
        (end_time + INTERVAL '2' DAY) + INTERVAL '90' MINUTE AS day_post_end_2,
        (end_time + INTERVAL '3' DAY) AS day_post_start_3,
        (end_time + INTERVAL '3' DAY) + INTERVAL '90' MINUTE AS day_post_end_3
    FROM event_window
)
SELECT
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time,
    SUM(
        CASE
            WHEN t.dropoff_ts >= e.pre_start_time
             AND t.dropoff_ts <  e.start_time
             AND t.end_location_id = e.zone_id
            THEN t.passenger_count ELSE 0
        END
    ) AS taxi_inbound_pre,
    SUM(
        CASE
            WHEN t.pickup_ts >= e.end_time
             AND t.pickup_ts <  e.post_end_time
             AND t.start_location_id = e.zone_id
            THEN t.passenger_count ELSE 0
        END
    ) AS taxi_outbound_post,
    (
        SUM(CASE WHEN t.dropoff_ts >= e.day_pre_start_1 AND t.dropoff_ts < e.day_pre_end_1 
                  AND t.end_location_id = e.zone_id THEN t.passenger_count ELSE 0 END) +
        SUM(CASE WHEN t.dropoff_ts >= e.day_pre_start_2 AND t.dropoff_ts < e.day_pre_end_2 
                  AND t.end_location_id = e.zone_id THEN t.passenger_count ELSE 0 END) +
        SUM(CASE WHEN t.dropoff_ts >= e.day_pre_start_3 AND t.dropoff_ts < e.day_pre_end_3 
                  AND t.end_location_id = e.zone_id THEN t.passenger_count ELSE 0 END)
    ) / 3.0 AS avg_taxi_inbound_baseline,
    (
        SUM(CASE WHEN t.pickup_ts >= e.day_post_start_1 AND t.pickup_ts < e.day_post_end_1 
                  AND t.start_location_id = e.zone_id THEN t.passenger_count ELSE 0 END) +
        SUM(CASE WHEN t.pickup_ts >= e.day_post_start_2 AND t.pickup_ts < e.day_post_end_2 
                  AND t.start_location_id = e.zone_id THEN t.passenger_count ELSE 0 END) +
        SUM(CASE WHEN t.pickup_ts >= e.day_post_start_3 AND t.pickup_ts < e.day_post_end_3 
                  AND t.start_location_id = e.zone_id THEN t.passenger_count ELSE 0 END)
    ) / 3.0 AS avg_taxi_outbound_baseline
FROM baseline_days e
LEFT JOIN taxi_table t
    ON (
        (t.dropoff_ts BETWEEN e.pre_start_time AND e.start_time AND t.end_location_id = e.zone_id)
        OR (t.pickup_ts BETWEEN e.end_time AND e.post_end_time AND t.start_location_id = e.zone_id)
        OR (t.dropoff_ts BETWEEN e.day_pre_start_1 AND e.day_pre_end_1 AND t.end_location_id = e.zone_id)
        OR (t.dropoff_ts BETWEEN e.day_pre_start_2 AND e.day_pre_end_2 AND t.end_location_id = e.zone_id)
        OR (t.dropoff_ts BETWEEN e.day_pre_start_3 AND e.day_pre_end_3 AND t.end_location_id = e.zone_id)
        OR (t.pickup_ts BETWEEN e.day_post_start_1 AND e.day_post_end_1 AND t.start_location_id = e.zone_id)
        OR (t.pickup_ts BETWEEN e.day_post_start_2 AND e.day_post_end_2 AND t.start_location_id = e.zone_id)
        OR (t.pickup_ts BETWEEN e.day_post_start_3 AND e.day_post_end_3 AND t.start_location_id = e.zone_id)
    )
   AND t.dropoff_ts >= DATE '2024-01-01'
   AND t.dropoff_ts <  DATE '2025-01-01'
GROUP BY
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time;


-- CREATE TABLE sampled_events_taxi (
--     event_id INT,
--     event_name STRING,
--     event_type STRING,
--     zone_id INT,
--     start_time TIMESTAMP,
--     end_time TIMESTAMP,
--     taxi_inbound_pre BIGINT,
--     taxi_outbound_post BIGINT,
--     avg_taxi_inbound_baseline DOUBLE,
--     avg_taxi_outbound_baseline DOUBLE
-- )
-- STORED AS ORC;

-- INSERT INTO sampled_events_taxi VALUES
-- (793006, 'Soccer -Regulation', 'Sport - Youth', 195, '2024-11-07 16:00:00.000', '2024-11-07 20:00:00.000', 85, 97, 74.7, 149.3),
-- (751172, 'Softball - Adults', 'Sport - Adult', 43, '2024-07-11 18:00:00.000', '2024-07-11 20:00:00.000', 219, 246, 254.7, 343.3),
-- (751570, 'Lacrosse', 'Sport - Youth', 195, '2024-02-08 15:00:00.000', '2024-02-08 17:30:00.000', 62, 102, 61.7, 136.0),
-- (793055, 'Soccer - Non Regulation', 'Sport - Youth', 16, '2024-11-07 15:00:00.000', '2024-11-07 18:00:00.000', 73, 116, 86.3, 100.0),
-- (806389, 'Universal Kids Soccer', 'Special Event', 43, '2024-10-10 09:00:00.000', '2024-10-10 12:00:00.000', 219, 356, 166.0, 379.0),
-- (810977, 'Soccer - Non Regulation', 'Sport - Youth', 59, '2024-10-10 15:00:00.000', '2024-10-10 17:30:00.000', 6, 16, 5.7, 8.7),
-- (761847, 'Soccer - Non Regulation', 'Sport - Youth', 93, '2024-08-08 13:30:00.000', '2024-08-08 18:00:00.000', 38, 28, 39.3, 42.0),
-- (776844, 'summer series', 'Special Event', 240, '2024-07-11 17:00:00.000', '2024-07-11 20:00:00.000', 31, 22, 27.7, 36.3),
-- (797622, 'Celebration', 'Special Event', 43, '2024-08-08 17:00:00.000', '2024-08-08 19:00:00.000', 240, 184, 238.3, 350.0),
-- (757989, 'Celebration', 'Special Event', 43, '2024-08-08 12:00:00.000', '2024-08-08 14:00:00.000', 508, 419, 384.7, 459.3),
-- (810920, '10-10-24 A Day in the Life of the Hudson and Harbor at Coney Island Pier', 'Special Event', 55, '2024-10-10 10:00:00.000', '2024-10-10 13:00:00.000', 156, 112, 140.7, 119.7),
-- (751766, 'Soccer - Non Regulation', 'Sport - Youth', 8, '2024-07-11 16:00:00.000', '2024-07-11 18:00:00.000', 11, 7, 3.0, 6.0);


CREATE TABLE sampled_events_subway AS
WITH event_window AS (
    SELECT
        event_id,
        event_name,
        event_type,
        start_location_id AS zone_id,
        start_time,
        end_time,
        start_time - INTERVAL '90' MINUTE AS pre_start_time,
        end_time   + INTERVAL '90' MINUTE AS post_end_time
    FROM sampled_events_12
),
baseline_days AS (
    SELECT 
        event_id,
        event_name,
        event_type,
        zone_id,
        start_time,
        end_time,
        pre_start_time,
        post_end_time,
        (start_time - INTERVAL '1' DAY) - INTERVAL '90' MINUTE AS day_pre_start_1,
        (start_time - INTERVAL '1' DAY) AS day_pre_end_1,
        (start_time - INTERVAL '2' DAY) - INTERVAL '90' MINUTE AS day_pre_start_2,
        (start_time - INTERVAL '2' DAY) AS day_pre_end_2,
        (start_time - INTERVAL '3' DAY) - INTERVAL '90' MINUTE AS day_pre_start_3,
        (start_time - INTERVAL '3' DAY) AS day_pre_end_3,
        (end_time + INTERVAL '1' DAY) AS day_post_start_1,
        (end_time + INTERVAL '1' DAY) + INTERVAL '90' MINUTE AS day_post_end_1,
        (end_time + INTERVAL '2' DAY) AS day_post_start_2,
        (end_time + INTERVAL '2' DAY) + INTERVAL '90' MINUTE AS day_post_end_2,
        (end_time + INTERVAL '3' DAY) AS day_post_start_3,
        (end_time + INTERVAL '3' DAY) + INTERVAL '90' MINUTE AS day_post_end_3
    FROM event_window
)
SELECT
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time,
    SUM(
        CASE
            WHEN s.transit_timestamp >= e.pre_start_time
             AND s.transit_timestamp <  e.start_time
             AND s.destination_location_id = e.zone_id
            THEN s.estimated_average_ridership ELSE 0
        END
    ) AS subway_inbound_pre,
    SUM(
        CASE
            WHEN s.transit_timestamp >= e.end_time
             AND s.transit_timestamp <  e.post_end_time
             AND s.origin_location_id = e.zone_id
            THEN s.estimated_average_ridership ELSE 0
        END
    ) AS subway_outbound_post,
    (
        SUM(CASE WHEN s.transit_timestamp >= e.day_pre_start_1 AND s.transit_timestamp < e.day_pre_end_1 
                  AND s.destination_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END) +
        SUM(CASE WHEN s.transit_timestamp >= e.day_pre_start_2 AND s.transit_timestamp < e.day_pre_end_2 
                  AND s.destination_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END) +
        SUM(CASE WHEN s.transit_timestamp >= e.day_pre_start_3 AND s.transit_timestamp < e.day_pre_end_3 
                  AND s.destination_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END)
    ) / 3.0 AS avg_subway_inbound_baseline,
    (
        SUM(CASE WHEN s.transit_timestamp >= e.day_post_start_1 AND s.transit_timestamp < e.day_post_end_1 
                  AND s.origin_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END) +
        SUM(CASE WHEN s.transit_timestamp >= e.day_post_start_2 AND s.transit_timestamp < e.day_post_end_2 
                  AND s.origin_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END) +
        SUM(CASE WHEN s.transit_timestamp >= e.day_post_start_3 AND s.transit_timestamp < e.day_post_end_3 
                  AND s.origin_location_id = e.zone_id
                  THEN s.estimated_average_ridership ELSE 0 END)
    ) / 3.0 AS avg_subway_outbound_baseline
FROM baseline_days e
LEFT JOIN ridership_mta_table s
    ON (
        (s.transit_timestamp BETWEEN e.pre_start_time AND e.start_time 
         AND s.destination_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.end_time AND e.post_end_time 
            AND s.origin_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_pre_start_1 AND e.day_pre_end_1 
            AND s.destination_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_pre_start_2 AND e.day_pre_end_2 
            AND s.destination_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_pre_start_3 AND e.day_pre_end_3 
            AND s.destination_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_post_start_1 AND e.day_post_end_1 
            AND s.origin_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_post_start_2 AND e.day_post_end_2 
            AND s.origin_location_id = e.zone_id)
        OR (s.transit_timestamp BETWEEN e.day_post_start_3 AND e.day_post_end_3 
            AND s.origin_location_id = e.zone_id)
    )
   AND s.transit_timestamp >= TIMESTAMP '2024-01-01 00:00:00'
   AND s.transit_timestamp <  TIMESTAMP '2025-01-01 00:00:00'
GROUP BY
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time;

-- CREATE TABLE sampled_events_subway (
--     event_id INT,
--     event_name STRING,
--     event_type STRING,
--     zone_id INT,
--     start_time TIMESTAMP,
--     end_time TIMESTAMP,
--     subway_inbound_pre DOUBLE,
--     subway_outbound_post DOUBLE,
--     avg_subway_inbound_baseline DOUBLE,
--     avg_subway_outbound_baseline DOUBLE
-- )
-- STORED AS ORC;

-- INSERT INTO sampled_events_subway VALUES
-- (751172, 'Softball - Adults', 'Sport - Adult', 43, '2024-07-11 18:00:00.000', '2024-07-11 20:00:00.000', 3419.8761000000013, 3139.4060999999965, 3913.6129333333333, 2699.329733333333),
-- (793006, 'Soccer -Regulation', 'Sport - Youth', 195, '2024-11-07 16:00:00.000', '2024-11-07 20:00:00.000', 0.0, 0.0, 0.0, 0.0),
-- (810920, '10-10-24 A Day in the Life of the Hudson and Harbor at Coney Island Pier', 'Special Event', 55, '2024-10-10 10:00:00.000', '2024-10-10 13:00:00.000', 562.9011999999999, 1672.0225999999996, 529.4570666666667, 1641.5806),
-- (757989, 'Celebration', 'Special Event', 43, '2024-08-08 12:00:00.000', '2024-08-08 14:00:00.000', 2976.473200000001, 7409.896199999993, 2573.4710333333333, 6315.285833333338),
-- (797622, 'Celebration', 'Special Event', 43, '2024-08-08 17:00:00.000', '2024-08-08 19:00:00.000', 2844.3942, 4141.800000000005, 2637.5544000000004, 3622.3161333333323),
-- (751766, 'Soccer - Non Regulation', 'Sport - Youth', 8, '2024-07-11 16:00:00.000', '2024-07-11 18:00:00.000', 0.0, 0.0, 0.0, 0.0),
-- (761847, 'Soccer - Non Regulation', 'Sport - Youth', 93, '2024-08-08 13:30:00.000', '2024-08-08 18:00:00.000', 2441.1860000000015, 2907.452199999999, 2399.8849999999993, 2506.6378000000004),
-- (776844, 'summer series', 'Special Event', 240, '2024-07-11 17:00:00.000', '2024-07-11 20:00:00.000', 0.0, 0.0, 0.0, 0.0),
-- (806389, 'Universal Kids Soccer', 'Special Event', 43, '2024-10-10 09:00:00.000', '2024-10-10 12:00:00.000', 5401.621400000002, 4998.520400000004, 5809.8997999999965, 5925.352399999999),
-- (810977, 'Soccer - Non Regulation', 'Sport - Youth', 59, '2024-10-10 15:00:00.000', '2024-10-10 17:30:00.000', 0.0, 0.0, 0.0, 0.0),
-- (793055, 'Soccer - Non Regulation', 'Sport - Youth', 16, '2024-11-07 15:00:00.000', '2024-11-07 18:00:00.000', 0.0, 0.0, 0.0, 0.0),
-- (751570, 'Lacrosse', 'Sport - Youth', 195, '2024-02-08 15:00:00.000', '2024-02-08 17:30:00.000', 0.0, 0.0, 0.0, 0.0);


CREATE TABLE sampled_events_citibike AS
WITH event_window AS (
    SELECT
        event_id,
        event_name,
        event_type,
        start_location_id AS zone_id,
        start_time,
        end_time,
        start_time - INTERVAL '90' MINUTE AS pre_start_time,
        end_time   + INTERVAL '90' MINUTE AS post_end_time
    FROM sampled_events_12
),
baseline_days AS (
    SELECT 
        event_id,
        event_name,
        event_type,
        zone_id,
        start_time,
        end_time,
        pre_start_time,
        post_end_time,
        (start_time - INTERVAL '1' DAY) - INTERVAL '90' MINUTE AS day_pre_start_1,
        (start_time - INTERVAL '1' DAY) AS day_pre_end_1,
        (start_time - INTERVAL '2' DAY) - INTERVAL '90' MINUTE AS day_pre_start_2,
        (start_time - INTERVAL '2' DAY) AS day_pre_end_2,
        (start_time - INTERVAL '3' DAY) - INTERVAL '90' MINUTE AS day_pre_start_3,
        (start_time - INTERVAL '3' DAY) AS day_pre_end_3,
        (end_time + INTERVAL '1' DAY) AS day_post_start_1,
        (end_time + INTERVAL '1' DAY) + INTERVAL '90' MINUTE AS day_post_end_1,
        (end_time + INTERVAL '2' DAY) AS day_post_start_2,
        (end_time + INTERVAL '2' DAY) + INTERVAL '90' MINUTE AS day_post_end_2,
        (end_time + INTERVAL '3' DAY) AS day_post_start_3,
        (end_time + INTERVAL '3' DAY) + INTERVAL '90' MINUTE AS day_post_end_3
    FROM event_window
)
SELECT
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time,
    COUNT(
        CASE
            WHEN c.ended_at >= e.pre_start_time
             AND c.ended_at <  e.start_time
             AND c.end_location_id = e.zone_id
            THEN 1 ELSE NULL
        END
    ) AS citibike_inbound_pre,
    COUNT(
        CASE
            WHEN c.started_at >= e.end_time
             AND c.started_at <  e.post_end_time
             AND c.start_location_id = e.zone_id
            THEN 1 ELSE NULL
        END
    ) AS citibike_outbound_post,
    (
        COUNT(CASE WHEN c.ended_at >= e.day_pre_start_1 AND c.ended_at < e.day_pre_end_1 
                    AND c.end_location_id = e.zone_id THEN 1 ELSE NULL END) +
        COUNT(CASE WHEN c.ended_at >= e.day_pre_start_2 AND c.ended_at < e.day_pre_end_2 
                    AND c.end_location_id = e.zone_id THEN 1 ELSE NULL END) +
        COUNT(CASE WHEN c.ended_at >= e.day_pre_start_3 AND c.ended_at < e.day_pre_end_3 
                    AND c.end_location_id = e.zone_id THEN 1 ELSE NULL END)
    ) / 3.0 AS avg_citibike_inbound_baseline,
    (
        COUNT(CASE WHEN c.started_at >= e.day_post_start_1 AND c.started_at < e.day_post_end_1 
                    AND c.start_location_id = e.zone_id THEN 1 ELSE NULL END) +
        COUNT(CASE WHEN c.started_at >= e.day_post_start_2 AND c.started_at < e.day_post_end_2 
                    AND c.start_location_id = e.zone_id THEN 1 ELSE NULL END) +
        COUNT(CASE WHEN c.started_at >= e.day_post_start_3 AND c.started_at < e.day_post_end_3 
                    AND c.start_location_id = e.zone_id THEN 1 ELSE NULL END)
    ) / 3.0 AS avg_citibike_outbound_baseline
FROM baseline_days e
LEFT JOIN citibike_table c
    ON (
        (c.ended_at BETWEEN e.pre_start_time AND e.start_time AND c.end_location_id = e.zone_id)
        OR (c.started_at BETWEEN e.end_time AND e.post_end_time AND c.start_location_id = e.zone_id)
        OR (c.ended_at BETWEEN e.day_pre_start_1 AND e.day_pre_end_1 AND c.end_location_id = e.zone_id)
        OR (c.ended_at BETWEEN e.day_pre_start_2 AND e.day_pre_end_2 AND c.end_location_id = e.zone_id)
        OR (c.ended_at BETWEEN e.day_pre_start_3 AND e.day_pre_end_3 AND c.end_location_id = e.zone_id)
        OR (c.started_at BETWEEN e.day_post_start_1 AND e.day_post_end_1 AND c.start_location_id = e.zone_id)
        OR (c.started_at BETWEEN e.day_post_start_2 AND e.day_post_end_2 AND c.start_location_id = e.zone_id)
        OR (c.started_at BETWEEN e.day_post_start_3 AND e.day_post_end_3 AND c.start_location_id = e.zone_id)
    )
   AND c.started_at >= TIMESTAMP '2024-01-01 00:00:00'
   AND c.started_at <  TIMESTAMP '2025-01-01 00:00:00'
GROUP BY
    e.event_id,
    e.event_name,
    e.event_type,
    e.zone_id,
    e.start_time,
    e.end_time;


-- CREATE TABLE sampled_events_citibike (
--     event_id INT,
--     event_name STRING,
--     event_type STRING,
--     zone_id INT,
--     start_time TIMESTAMP,
--     end_time TIMESTAMP,
--     citibike_inbound_pre BIGINT,
--     citibike_outbound_post BIGINT,
--     avg_citibike_inbound_baseline DOUBLE,
--     avg_citibike_outbound_baseline DOUBLE
-- )
-- STORED AS ORC;

-- INSERT INTO sampled_events_citibike VALUES
-- (761847, 'Soccer - Non Regulation', 'Sport - Youth', 93, '2024-08-08 13:30:00.000', '2024-08-08 18:00:00.000', 0, 5, 2.0, 6.0),
-- (776844, 'summer series', 'Special Event', 240, '2024-07-11 17:00:00.000', '2024-07-11 20:00:00.000', 6, 0, 1.7, 1.7),
-- (806389, 'Universal Kids Soccer', 'Special Event', 43, '2024-10-10 09:00:00.000', '2024-10-10 12:00:00.000', 301, 281, 322.0, 518.7),
-- (810977, 'Soccer - Non Regulation', 'Sport - Youth', 59, '2024-10-10 15:00:00.000', '2024-10-10 17:30:00.000', 3, 4, 4.7, 5.0),
-- (751570, 'Lacrosse', 'Sport - Youth', 195, '2024-02-08 15:00:00.000', '2024-02-08 17:30:00.000', 15, 23, 13.7, 31.0),
-- (793055, 'Soccer - Non Regulation', 'Sport - Youth', 16, '2024-11-07 15:00:00.000', '2024-11-07 18:00:00.000', 0, 0, 0.0, 0.0),
-- (751172, 'Softball - Adults', 'Sport - Adult', 43, '2024-07-11 18:00:00.000', '2024-07-11 20:00:00.000', 381, 214, 311.3, 183.0),
-- (793006, 'Soccer -Regulation', 'Sport - Youth', 195, '2024-11-07 16:00:00.000', '2024-11-07 20:00:00.000', 34, 14, 52.7, 25.3),
-- (751766, 'Soccer - Non Regulation', 'Sport - Youth', 8, '2024-07-11 16:00:00.000', '2024-07-11 18:00:00.000', 13, 14, 9.7, 25.0),
-- (797622, 'Celebration', 'Special Event', 43, '2024-08-08 17:00:00.000', '2024-08-08 19:00:00.000', 133, 49, 233.7, 259.0),
-- (757989, 'Celebration', 'Special Event', 43, '2024-08-08 12:00:00.000', '2024-08-08 14:00:00.000', 70, 137, 182.7, 509.3),
-- (810920, '10-10-24 A Day in the Life of the Hudson and Harbor at Coney Island Pier', 'Special Event', 55, '2024-10-10 10:00:00.000', '2024-10-10 13:00:00.000', 0, 0, 0.0, 0.0);


CREATE TABLE event_transit_summary AS
SELECT
    t.event_id,
    t.event_name,
    t.event_type,
    t.zone_id,
    t.start_time,
    t.end_time,
    COALESCE(t.taxi_inbound_pre, 0) AS taxi_inbound_pre,
    COALESCE(t.taxi_outbound_post, 0) AS taxi_outbound_post,
    COALESCE(t.avg_taxi_inbound_baseline, 0) AS avg_taxi_inbound_baseline,
    COALESCE(t.avg_taxi_outbound_baseline, 0) AS avg_taxi_outbound_baseline,
    COALESCE(s.subway_inbound_pre, 0) AS subway_inbound_pre,
    COALESCE(s.subway_outbound_post, 0) AS subway_outbound_post,
    COALESCE(s.avg_subway_inbound_baseline, 0) AS avg_subway_inbound_baseline,
    COALESCE(s.avg_subway_outbound_baseline, 0) AS avg_subway_outbound_baseline,
    COALESCE(c.citibike_inbound_pre, 0) AS citibike_inbound_pre,
    COALESCE(c.citibike_outbound_post, 0) AS citibike_outbound_post,
    COALESCE(c.avg_citibike_inbound_baseline, 0) AS avg_citibike_inbound_baseline,
    COALESCE(c.avg_citibike_outbound_baseline, 0) AS avg_citibike_outbound_baseline,
    COALESCE(t.taxi_inbound_pre, 0) + COALESCE(s.subway_inbound_pre, 0) + COALESCE(c.citibike_inbound_pre, 0) AS total_inbound_pre,
    COALESCE(t.taxi_outbound_post, 0) + COALESCE(s.subway_outbound_post, 0) + COALESCE(c.citibike_outbound_post, 0) AS total_outbound_post,
    COALESCE(t.avg_taxi_inbound_baseline, 0) + COALESCE(s.avg_subway_inbound_baseline, 0) + COALESCE(c.avg_citibike_inbound_baseline, 0) AS avg_total_inbound_baseline,
    COALESCE(t.avg_taxi_outbound_baseline, 0) + COALESCE(s.avg_subway_outbound_baseline, 0) + COALESCE(c.avg_citibike_outbound_baseline, 0) AS avg_total_outbound_baseline,
    (COALESCE(t.taxi_inbound_pre, 0) + COALESCE(s.subway_inbound_pre, 0) + COALESCE(c.citibike_inbound_pre, 0)) - 
    (COALESCE(t.avg_taxi_inbound_baseline, 0) + COALESCE(s.avg_subway_inbound_baseline, 0) + COALESCE(c.avg_citibike_inbound_baseline, 0)) AS total_inbound_delta,
    (COALESCE(t.taxi_outbound_post, 0) + COALESCE(s.subway_outbound_post, 0) + COALESCE(c.citibike_outbound_post, 0)) - 
    (COALESCE(t.avg_taxi_outbound_baseline, 0) + COALESCE(s.avg_subway_outbound_baseline, 0) + COALESCE(c.avg_citibike_outbound_baseline, 0)) AS total_outbound_delta
FROM sampled_events_taxi t
LEFT JOIN sampled_events_subway s ON t.event_id = s.event_id
LEFT JOIN sampled_events_citibike c ON t.event_id = c.event_id
ORDER BY t.event_id;

