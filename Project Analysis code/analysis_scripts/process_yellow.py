from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

spark = SparkSession.builder \
    .appName("Process Yellow Taxi Data") \
    .getOrCreate()

print("Processing Yellow taxi data...")

df = spark.read.json("taxi/cleaned/yellow")

print(f"Total records: {df.count()}")

df_transformed = df \
    .withColumn("pickup_ts", 
                from_unixtime(col("tpep_pickup_datetime") / 1000000, 
                             "yyyy-MM-dd HH:mm:ss.SSS")) \
    .withColumn("dropoff_ts", 
                from_unixtime(col("tpep_dropoff_datetime") / 1000000, 
                             "yyyy-MM-dd HH:mm:ss.SSS")) \
    .withColumnRenamed("PULocationID", "start_location_id") \
    .withColumnRenamed("DOLocationID", "end_location_id") \
    .select("pickup_ts", "dropoff_ts", "start_location_id", 
            "end_location_id", "trip_distance", "passenger_count", "taxi_type")

print("Sample transformed data:")
df_transformed.show(5, truncate=False)

df_transformed.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("taxi/final/yellow")

print("Yellow taxi processing completed!")
print("Output saved to: taxi/final/yellow")

spark.stop()