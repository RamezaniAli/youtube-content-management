from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Callbacks
def process_data_from_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    hook = MongoHook(mongo_conn_id=conn_id)
    collection = hook.get_collection('utube', 'videos')
    record_count = collection.count_documents({})
    return record_count


# DAG and its tasks
with DAG(
    dag_id='mongo_record_count_dag',
    description='Count records in MongoDB collection',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data_from_mongo_task',
        python_callable=process_data_from_mongo,
        op_kwargs={'conn_id': 'oltp_mongo_conn'}
    )
