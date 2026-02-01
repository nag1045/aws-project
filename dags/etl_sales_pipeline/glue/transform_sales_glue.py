import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

# --------------------------------------------------
# Arguments (from Airflow / Glue job parameters)
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "execution_date"]
)

execution_date = args["execution_date"]

# --------------------------------------------------
# Spark / Glue Context
# --------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# S3 Paths
# --------------------------------------------------
input_path = (
    f"s3://nag-sales-data-bucket/landing/sales/"
    f"{execution_date}/sales.csv"
)

output_path = (
    f"s3://nag-sales-data-bucket/curated/sales/"
    f"{execution_date}/"
)

# --------------------------------------------------
# Read raw CSV
# --------------------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

# --------------------------------------------------
# Transformations
# --------------------------------------------------
df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("total_amount").isNotNull())
    .withColumn("total_amount", col("total_amount").cast("decimal(12,2)"))
    .withColumn("order_date", to_date(col("order_date")))
)

# print(f"Row count = {df_clean.count()}")

# --------------------------------------------------
# Write curated Parquet
# --------------------------------------------------
(
    df_clean.write
    .mode("overwrite")
    .parquet(output_path)
)

job.commit()
