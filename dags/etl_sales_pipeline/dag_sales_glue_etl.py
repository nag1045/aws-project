from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "Nag-DE",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sales_etl_spark_to_redshift",
    default_args=default_args,
    description="ETL pipeline: S3 -> Glue Spark -> S3 -> Redshift",
    schedule_interval=None,          # 👈 manual trigger for now
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "spark", "redshift"],
) as dag:

    # 1️⃣ Wait for raw CSV file (hard-coded path)
    wait_for_raw_file = S3KeySensor(
        task_id="wait_for_raw_sales_file",
        bucket_name="nag-sales-data-bucket",
        bucket_key="landing/sales/sales.csv",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
    )

    # 2️⃣ Run Glue job (NO script args)
    glue_transform = GlueJobOperator(
        task_id="run_glue_sales_transform",
        job_name="sales-etl-glue-job",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    # 3️⃣ Create Redshift table
    create_table = PostgresOperator(
        task_id="create_sales_table",
        postgres_conn_id="redshift_default",
        sql="sql/create_table.sql",
    )

    # 4️⃣ Copy curated data into Redshift
    copy_to_redshift = PostgresOperator(
        task_id="copy_sales_to_redshift",
        postgres_conn_id="redshift_default",
        sql="sql/copy_sales.sql",
    )

    # DAG order
    wait_for_raw_file >> glue_transform >> create_table >> copy_to_redshift
