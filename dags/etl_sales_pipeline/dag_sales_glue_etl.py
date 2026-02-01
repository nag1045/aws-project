from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor  # A sensor is a special task that waits (keeps checking) until a condition becomes true
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args={
    "owner":"Nag-DE",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id='sales_etl_spark_to_redshift',
    default_args=default_args,
    description="ETL pipeline: S3 -> Spark -> S3 -> Redshift",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl","spark","redshift"],
) as dag:
    wait_for_raw_file=S3KeySensor (
        task_id="wait_for_raw_sales_file",
        bucket_name="nag-sales-data-bucket",
        bucket_key="landing/sales/{{ ds }}/sales.csv"    ,                       # This is the exact object (file) path inside S3
        aws_conn_id="aws_default",      # aws_default is an Airflow connection configured in UI → Admin → Connections
        poke_interval=60,               # Sensor will check S3 every 60 seconds
        timeout=60*60,                  # Maximum time the sensor will wait before failing -> 60*60=3600 seconds = 1 hour
    )

    glue_transform=GlueJobOperator(
        task_id="run_glue_sales_transform",
        job_name="sales-etl-glue-job",
        script_args={
            "--execution_date":"{{ds}}"
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        
    )
    create_table = PostgresOperator(
        task_id="create_sales_table",
        postgres_conn_id="redshift_default",
        sql="sql/create_table.sql",
    )
    copy_to_redshift = PostgresOperator(
        task_id="copy_sales_to_redshift",
        postgres_conn_id="redshift_default",
        sql="sql/copy_sales.sql",
    )


    wait_for_raw_file>>glue_transform>>create_table>>copy_to_redshift