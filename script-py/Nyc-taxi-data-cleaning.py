from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, round, when

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxiTransformation") \
    .getOrCreate()

# 2. Load Data from CSV
# Path: s3://training-s3-data-lake/raw/yellow_tripdata_2024-01.csv
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3://training-s3-data-lake/raw/yellow_tripdata_*.csv")

# 3. Data Cleaning
cleaned_df = df.filter(
    (col("passenger_count") > 0) & 
    (col("trip_distance") > 0) & 
    (col("fare_amount") >= 2.5) &
    (col("total_amount") > 0)
)

# 4. Feature Engineering
transformed_df = cleaned_df \
    .withColumn("trip_duration_min", 
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    ) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
    .withColumn("tip_percentage", 
        round((col("tip_amount") / col("fare_amount")) * 100, 2)
    ) \
    .withColumn("is_high_tip", when(col("tip_percentage") > 20, 1).otherwise(0))

# 5. Filtering out unrealistic durations (e.g., trips > 5 hours or < 1 min)
final_df = transformed_df.filter((col("trip_duration_min") > 1) & (col("trip_duration_min") < 300))

# 6. Show results
final_df.select("tpep_pickup_datetime", "trip_distance", "fare_amount", "trip_duration_min", "tip_percentage").show(5)

# 7. Write transformed data back to S3 (Partitioned by pickup_hour for performance)
final_df.write.mode("overwrite").partitionBy("pickup_hour").parquet("s3://training-s3-data-lake/transformed-silver/yellow_taxi_enriched/")