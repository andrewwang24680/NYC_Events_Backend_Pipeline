from pathlib import Path
import geopandas as gpd
import pandas as pd

COLUMN_NAMES = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_type"
]
# Add new columns: passenger_count, start_location_id, end_location_id

input_path = input("Input File path: ").strip()
INPUT_DIR = Path(input_path)
if not INPUT_DIR.exists():
    print(f"ERROR: Path does not exist: {INPUT_DIR}")
    exit()

if not INPUT_DIR.is_dir():
    print(f"ERROR: Path is not a folder: {INPUT_DIR}")
    exit()

TAXI_ZONE_SHP = Path("taxi_zones.shp")

zones = gpd.read_file(TAXI_ZONE_SHP)
zones = zones.to_crs(epsg=4326)
zones = zones[["LocationID", "zone", "borough", "geometry"]]

def add_location_id_for_file(csv_path: Path, zones: gpd.GeoDataFrame):
    print(f"\nProcessing file: {csv_path.name} (full path: {csv_path})")
    trips = pd.read_csv(csv_path, header=None, names=COLUMN_NAMES)

    gdf_start = gpd.GeoDataFrame(
        trips.copy(),
        geometry=gpd.points_from_xy(trips["start_lng"], trips["start_lat"]),
        crs="EPSG:4326"
    )
    gdf_end = gpd.GeoDataFrame(
        trips.copy(),
        geometry=gpd.points_from_xy(trips["end_lng"], trips["end_lat"]),
        crs="EPSG:4326"
    )

    trips["passenger_count"] = 1

    start_join = gpd.sjoin(gdf_start, zones, how="left", predicate="within")
    end_join = gpd.sjoin(gdf_end, zones, how="left", predicate="within")

    trips["start_location_id"] = start_join["LocationID"]
    trips["end_location_id"] = end_join["LocationID"]

    missing_start = trips[trips["start_location_id"].isna()]
    if len(missing_start) > 0:
        print("\n WARNING: Missing start_location_id found!")
        for idx, row in missing_start.iterrows():
            print(
                f"  File: {csv_path.name}, Line: {idx+1}, "
                f"start_lat={row['start_lat']}, start_lng={row['start_lng']}"
            )

    missing_end = trips[trips["end_location_id"].isna()]
    if len(missing_end) > 0:
        print("\n WARNING: Missing end_location_id found!")
        for idx, row in missing_end.iterrows():
            print(
                f"  File: {csv_path.name}, Line: {idx+1}, "
                f"end_lat={row['end_lat']}, end_lng={row['end_lng']}"
            )

    trips.to_csv(csv_path, index=False, header=False)
    print(f"Overwritten file with LocationID: {csv_path}")

def main():
    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    # csv_files = sorted(INPUT_DIR.glob("part-m-*"))
    if not csv_files:
        print(f"No CSV files found in {INPUT_DIR}")
        return

    for csv_path in csv_files:
        add_location_id_for_file(csv_path, zones)

if __name__ == "__main__":
    main()




###################### SQL for table creation ######################
# For CSV

# CREATE EXTERNAL TABLE citibike_clean_csv (
#   ride_id            STRING,
#   rideable_type      STRING,
#   started_at         STRING, 
#   ended_at           STRING,
#   start_station_name STRING,
#   start_station_id   STRING,
#   end_station_name   STRING,
#   end_station_id     STRING,
#   start_lat          DOUBLE,
#   start_lng          DOUBLE,
#   end_lat            DOUBLE,
#   end_lng            DOUBLE,
#   member_type        STRING,
#   passenger_count    INT,
#   start_location_id  INT,
#   end_location_id    INT
# )
# PARTITIONED BY (year INT) 
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE;

# ALTER TABLE citibike_clean_csv
#   ADD PARTITION (year=2024)
#   LOCATION '/user/hx2487_nyu_edu/citibike/clean/2024';

# ALTER TABLE citibike_clean_csv
#   ADD PARTITION (year=2025)
#   LOCATION '/user/hx2487_nyu_edu/citibike/clean/2025';

# For ORC 

# CREATE EXTERNAL TABLE fact_citibike (
#   ride_id            STRING,
#   rideable_type      STRING,
#   started_at         TIMESTAMP, 
#   ended_at           TIMESTAMP,
#   start_station_name STRING,
#   start_station_id   STRING,
#   end_station_name   STRING,
#   end_station_id     STRING,
#   start_lat          DOUBLE,
#   start_lng          DOUBLE,
#   end_lat            DOUBLE,
#   end_lng            DOUBLE,
#   member_type        STRING,
#   passenger_count    INT,
#   start_location_id  INT,
#   end_location_id    INT
# )
# PARTITIONED BY (year INT, month INT)
# STORED AS ORC
# LOCATION '/user/hx2487_nyu_edu/citibike/fact_citibike/';

# SET hive.exec.dynamic.partition = true;
# SET hive.exec.dynamic.partition.mode = nonstrict;


# INSERT OVERWRITE TABLE fact_citibike
# PARTITION (year, month)
# SELECT
#   ride_id,
#   rideable_type,
#   from_unixtime(unix_timestamp(started_at, 'yyyy-MM-dd HH:mm:ss.SSS')) AS started_at,
#   from_unixtime(unix_timestamp(ended_at,   'yyyy-MM-dd HH:mm:ss.SSS')) AS ended_at,
#   start_station_name,
#   start_station_id,
#   end_station_name,
#   end_station_id,
#   start_lat,
#   start_lng,
#   end_lat,
#   end_lng,
#   member_type,
#   passenger_count,
#   start_location_id,
#   end_location_id,
#   year,                                        
#   month(from_unixtime(unix_timestamp(started_at)))
# FROM citibike_clean_csv;


# DESCRIBE FORMATTED fact_citibike;
# SHOW PARTITIONS fact_citibike;
# SELECT COUNT(*) FROM fact_citibike;


# CREATE EXTERNAL TABLE citibike_table (
#   ride_id            STRING,
#   rideable_type      STRING,
#   started_at         TIMESTAMP,
#   ended_at           TIMESTAMP,
#   start_station_name STRING,
#   start_station_id   STRING,
#   end_station_name   STRING,
#   end_station_id     STRING,
#   start_lat          DOUBLE,
#   start_lng          DOUBLE,
#   end_lat            DOUBLE,
#   end_lng            DOUBLE,
#   member_type        STRING,
#   passenger_count    INT,
#   start_location_id  INT,
#   end_location_id    INT
# )
# PARTITIONED BY (year INT, month INT)
# STORED AS ORC
# LOCATION '/user/hx2487_nyu_edu/citibike/fact_citibike/';


# MSCK REPAIR TABLE citibike_table;
# SHOW PARTITIONS fact_citibike;


# SELECT
#   ride_id,
#   started_at,
#   end_location_id,
#   start_location_id,
#   year,
#   month
# FROM fact_citibike
# WHERE year = 2024
#   AND ride_id IN ('8B8C68779F8BF528', '9694D5311C7798A4', '5EE3DCB7CEB4831A', '45395AC6A6F61712', '655C1EE9BB37DE49'); 
