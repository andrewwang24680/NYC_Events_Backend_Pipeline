from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Merge Taxi CSV Files") \
    .getOrCreate()

taxi_types = ['green', 'yellow', 'fhvhv']

for taxi_type in taxi_types:
    print(f"Merging {taxi_type}...")
    
    df = spark.read \
        .option("header", "true") \
        .csv(f"taxi/final/{taxi_type}")
    
    print(f"{taxi_type} total records: {df.count()}")
    
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"taxi/merged/{taxi_type}")
    
    print(f"{taxi_type} merged successfully!\n")

print("All files merged!")
spark.stop()