import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_date

# Get job arguments
args = getResolvedOptions(sys.argv, ["execution_date"])
execution_date = args["execution_date"]

sc = SparkContext()
glueContext = GlueContext(sc)   
spark = glueContext.spark_session

input_path = f"s3://nag-sales-data-bucket/landing/sales/{execution_date}/"
output_path = f"s3://nag-sales-data-bucket/curated/sales/{execution_date}/"

# Read raw data
df = spark.read.option("header", "true").csv(input_path)

# Transform (ETL logic)
df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .filter(col("amount").isNotNull())
    .withColumn("amount", col("amount").cast("decimal(10,2)"))
    .withColumn("order_date", to_date(col("order_date")))
)

# Write curated data
df_clean.write.mode("overwrite").parquet(output_path)
