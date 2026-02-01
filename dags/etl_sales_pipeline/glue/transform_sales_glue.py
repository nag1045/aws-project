from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DecimalType, IntegerType

# --------------------------------------------------
# Glue / Spark Context
# --------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init("sales-etl-glue-job", {})   # Job name can be static

# --------------------------------------------------
# Hard-coded S3 paths
# --------------------------------------------------
input_path = "s3://nag-sales-data-bucket/landing/sales/sales.csv"
output_path = "s3://nag-sales-data-bucket/curated/sales/"

# --------------------------------------------------
# Read CSV
# --------------------------------------------------
df = (
    spark.read
    .option("header", "true")
    .csv(input_path)
)

# --------------------------------------------------
# Transform
# --------------------------------------------------
df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .withColumn("order_date", to_date(col("order_date")))
)

# Force Spark execution (good for debugging)
print(f"Row count = {df_clean.count()}")

# to avoid numeric value mismatch in redshift
df_clean = (
    df
    .filter(col("order_id").isNotNull())
    .withColumn(
        "order_date",
        to_date(col("order_date"), "dd-MM-yyyy")
    )
    .withColumn(
        "quantity",
        col("quantity").cast(IntegerType())
    )
    .withColumn(
        "price",
        col("price").cast(DecimalType(10, 2))
    )
    .withColumn(
        "total_amount",
        col("total_amount").cast(DecimalType(12, 2))
    )
)
# --------------------------------------------------
# Write Parquet
# --------------------------------------------------
df_clean.write.mode("overwrite").parquet(output_path)

job.commit()
