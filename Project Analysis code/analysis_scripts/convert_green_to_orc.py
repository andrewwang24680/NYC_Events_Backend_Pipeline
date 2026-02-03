from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder \
    .appName("Convert Green Taxi to ORC with Partitions") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv("taxi/merged/green")

print(f"Total records: {df.count():,}")

df_with_partitions = df \
    .withColumn("pickup_ts", col("pickup_ts").cast("timestamp")) \
    .withColumn("year", year(col("pickup_ts"))) \
    .withColumn("month", month(col("pickup_ts")))

df_with_partitions = df_with_partitions.filter(
    ((col("year") == 2024) & (col("month").between(1, 12))) |
    ((col("year") == 2025) & (col("month").between(1, 9)))
)

print(f"Records after filtering (2024/1-12, 2025/1-9): {df_with_partitions.count():,}")

# Check which year/month partitions exist
print("\nYear-month partitions:")
df_with_partitions.groupBy("year", "month").count().orderBy("year", "month").show(50)

# Convert data types of other numeric columns
df_final = df_with_partitions \
    .withColumn("dropoff_ts", col("dropoff_ts").cast("timestamp")) \
    .withColumn("start_location_id", col("start_location_id").cast("int")) \
    .withColumn("end_location_id", col("end_location_id").cast("int")) \
    .withColumn("trip_distance", col("trip_distance").cast("double")) \
    .withColumn("passenger_count", col("passenger_count").cast("int"))

# Write to ORC format, partitioned by year and month
print("\nWriting to taxi/partitioned/green/")

df_final.write \
    .partitionBy("year", "month") \
    .format("orc") \
    .mode("overwrite") \
    .save("taxi/partitioned/green")

print("\nDone.")

spark.stop()