import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# 0. Initialize Contexts and Spark Session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session  # <--- This defines 'spark'
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- YOUR EXISTING LOGIC STARTS HERE ---

# 1. Read the Silver Data
silver_df = spark.read.parquet("s3://training-s3-data-lake/transformed-silver/yellow_taxi_enriched/")

# 2. Create the Golden Record
gold_df = silver_df.groupBy("PULocationID", "pickup_hour", "day_of_week") \
    .agg(
        F.count("vendorid").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_trip_duration")
    ) \
    .filter(F.col("total_trips") > 5)

# 3. Write to Gold Layer
gold_df.write.mode("overwrite") \
    .parquet("s3://training-s3-data-lake/gold/hourly_taxi_performance/")

# 4. Commit the Job (Required for Glue to mark the job as 'Succeeded')
job.commit()