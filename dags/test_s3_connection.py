from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago


# Callbacks
def list_arvan_buckets():
    s3_hook = S3Hook(aws_conn_id='arvan_s3_conn')
    buckets = s3_hook.list_buckets()
    print(f"Buckets in ArvanCloud: {buckets}")
    return buckets


# DAG and its tasks
with DAG(
    dag_id='list_arvan_buckets_dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    list_buckets_task = PythonOperator(
        task_id='list_arvan_buckets',
        python_callable=list_arvan_buckets,
    )

    list_buckets_task
