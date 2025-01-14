import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook


# Callbacks
def create_videos_schema(**kwargs):
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    conn = BaseHook.get_connection(clickhouse_conn_id)
    host = conn.host
    port = conn.port
    username = conn.login
    password = conn.password
    database = 'bronze'
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )
    # Read SQL query file
    sql_file_path = '/opt/airflow/scripts/bronze_videos_schema.sql'
    with open(sql_file_path, 'r') as file:
        create_videos_table_query = file.read()
    # Execute the SQL query
    client.query(create_videos_table_query)
    return 'Done!'


def etl_data_from_mongo(**kwargs):
    mongo_conn_id = kwargs['mongo_conn_id']
    # Connect to MongoDB
    mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
    mongo_db = mongo_hook.get_conn()['utube']
    mongo_videos_collection = mongo_db['videos']
    documents = list(mongo_videos_collection.find().limit(10))
    return documents


# DAG and its tasks
with DAG(
    dag_id='load_bronze_videos_data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    create_videos_schema_task = PythonOperator(
        task_id='create_videos_schema_task',
        python_callable=create_videos_schema,
        provide_context=True,
        op_kwargs={'clickhouse_conn_id': 'wh_clickhouse_conn'}
    )

    etl_data_from_mongo_task = PythonOperator(
        task_id='etl_data_from_mongo_task',
        python_callable=etl_data_from_mongo,
        provide_context=True,
        op_kwargs={
            'mongo_conn_id': 'oltp_mongo_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    create_videos_schema_task >> etl_data_from_mongo_task >> dummy_task
