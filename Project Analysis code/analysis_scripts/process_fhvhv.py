from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime

spark = SparkSession.builder \
    .appName("Process FHVHV Taxi Data") \
    .getOrCreate()

print("Processing FHVHV data...")

df = spark.read.json("taxi/cleaned/fhvhv")

print(f"Total records: {df.count()}")

df_transformed = df \
    .withColumn("pickup_ts", 
                from_unixtime(col("pickup_datetime") / 1000000, 
                             "yyyy-MM-dd HH:mm:ss.SSS")) \
    .withColumn("dropoff_ts", 
                from_unixtime(col("dropoff_datetime") / 1000000, 
                             "yyyy-MM-dd HH:mm:ss.SSS")) \
    .withColumnRenamed("PULocationID", "start_location_id") \
    .withColumnRenamed("DOLocationID", "end_location_id") \
    .withColumnRenamed("trip_miles", "trip_distance") \
    .withColumn("passenger_count", lit(1)) \
    .select("pickup_ts", "dropoff_ts", "start_location_id", 
            "end_location_id", "trip_distance", "passenger_count", "taxi_type")

print("Sample transformed data:")
df_transformed.show(5, truncate=False)

df_transformed.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("taxi/final/fhvhv")

print("FHVHV processing completed!")
print("Output saved to: taxi/final/fhvhv")

spark.stop()