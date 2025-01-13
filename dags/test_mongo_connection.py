from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Callbacks
def process_data_from_mongo(**kwargs):
    try:
        from airflow.providers.mongo.hooks.mongo import MongoHook
        print("MongoHook importing Done........................")
    except ImportError as e:
        print(f"Error importing MongoHook: {e}")
    # conn_id = kwargs['conn_id']
    # hook = MongoHook(mongo_conn_id=conn_id)
    # collection = hook.get_collection('utube', 'videos')
    # record_count = collection.count()
    return "OLTP Mongo Connection ID!!"


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
    )
