import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_QUALITY_S3_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Load Data from Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "training-database", 
    table_name = "transformed-yellow_taxi_enriched_daab24e4cff1790ea531addd36ae9515"
)

# 2. Define Business Rules based on Data Dictionary [cite: 1, 6]
dq_ruleset = """
    Rules = [
        # VendorID must be 1, 2, 6, or 7 
        ColumnValues "VendorID" in [1, 2, 6, 7],
        
        # Trip distance must be positive 
        ColumnValues "trip_distance" >= 0,
        
        # RatecodeID must be within valid range (1-6 or 99) 
        ColumnValues "RatecodeID" in [1, 2, 3, 4, 5, 6, 99],
        
        # Payment Type must be valid (0-6) 
        ColumnValues "payment_type" in [0, 1, 2, 3, 4, 5, 6],
        
        # Fare amount cannot be negative 
        ColumnValues "fare_amount" >= 0,
        
        # Ensure mandatory timestamp fields are not null 
        IsComplete "tpep_pickup_datetime",
        IsComplete "tpep_dropoff_datetime"
    ]
"""

# 3. Evaluate Data Quality
dq_results = EvaluateDataQuality().process_rows(
    frame = datasource,
    ruleset = dq_ruleset,
    publishing_options = {
        "dataQualityEvaluationContext": "taxi_dq_context",
        "enableMetrics": True,
        "resultsS3Prefix": args['DATA_QUALITY_S3_PATH']
    }
)

# 4. Filter records based on DQ results
# This creates a "passed" frame and an "audit" frame for failures
passed_records = SelectFromCollection.apply(
    dfc = dq_results,
    key = "default_clicked_ruleset"
)
row_results = SelectFromCollection.apply(
    dfc = dq_results,
    key = "rowLevelOutcomes"
)
# Convert to Spark DataFrame to filter only the passed rows
# Glue DQ adds a column called "DataQualityEvaluationResult" automatically
passed_records_df = row_results.toDF().filter("DataQualityEvaluationResult = 'Passed'")
# Convert back to DynamicFrame for writing
passed_records_final = DynamicFrame.fromDF(passed_records_df, glueContext, "passed_records_final")
# 5. Write clean data to S3 Parquet
glueContext.write_dynamic_frame.from_options(
    frame = passed_records_final,
    connection_type = "s3",
    format = "glueparquet",
    connection_options = {"path": "s3://training-s3-data-lake/clean-taxi-data/etldata/"},
    format_options = {"compression": "snappy"}
)


job.commit()