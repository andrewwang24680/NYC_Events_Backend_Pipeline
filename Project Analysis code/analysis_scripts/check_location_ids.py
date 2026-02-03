from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Check Location IDs") \
    .getOrCreate()

taxi_types = ['green', 'yellow', 'fhvhv']

all_start_ids = set()
all_end_ids = set()

for taxi_type in taxi_types:
    print(f"\n{'='*60}")
    print(f"Checking {taxi_type.upper()}")
    print(f"{'='*60}")
    
    df = spark.read \
        .format("orc") \
        .load(f"taxi/partitioned/{taxi_type}")
    
    start_ids = df.select("start_location_id").distinct().rdd.flatMap(lambda x: x).collect()
    start_ids_set = set(start_ids)
    
    end_ids = df.select("end_location_id").distinct().rdd.flatMap(lambda x: x).collect()
    end_ids_set = set(end_ids)
    
    combined_ids = start_ids_set.union(end_ids_set)
    
    print(f"Unique start location IDs: {len(start_ids_set)}")
    print(f"Unique end location IDs: {len(end_ids_set)}")
    print(f"Total unique location IDs: {len(combined_ids)}")
    print(f"Min ID: {min(combined_ids)}, Max ID: {max(combined_ids)}")
    
    all_start_ids.update(start_ids_set)
    all_end_ids.update(end_ids_set)

print(f"\n{'='*60}")
print("OVERALL SUMMARY")
print(f"{'='*60}")

all_location_ids = all_start_ids.union(all_end_ids)
print(f"Total unique location IDs across all taxi types: {len(all_location_ids)}")
print(f"Min ID: {min(all_location_ids)}, Max ID: {max(all_location_ids)}")

sorted_ids = sorted(all_location_ids)
print(f"\nAll location IDs (first 50): {sorted_ids[:50]}")
print(f"All location IDs (last 50): {sorted_ids[-50:]}")

ids_in_range = [id for id in sorted_ids if 1 <= id <= 263]
print(f"\nLocation IDs in range [1-263]: {len(ids_in_range)}")

ids_out_range = [id for id in sorted_ids if id < 1 or id > 263]
if ids_out_range:
    print(f"WARNING: Found {len(ids_out_range)} IDs outside [1-263] range: {ids_out_range}")

spark.stop()