from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago


# Callbacks
def list_arvan_objects():
    s3_hook = S3Hook(aws_conn_id='arvan_s3_conn')
    bucket = s3_hook.get_bucket(bucket_name='qbc')
    print(f"Bucket Name: {bucket.name}")
    for obj in bucket.objects.all():
        print(f" - {obj.key} (Size: {obj.size} bytes)")


# DAG and its tasks
with DAG(
    dag_id='test_s3_connection_dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    list_bucket_objects_task = PythonOperator(
        task_id='list_bucket_objects_task',
        python_callable=list_arvan_objects,
    )

    list_bucket_objects_task
