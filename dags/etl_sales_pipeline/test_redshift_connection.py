from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = "test_redshift_connection"

with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "redshift"],
) as dag:

    test_redshift = PostgresOperator(
        task_id="test_redshift_connection",
        postgres_conn_id="redshift_default",
        sql="SELECT 1;",
    )

    test_redshift
