import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime


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
    # Connect to Clickhouse
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_host = clickhouse_connection.host
    clickhouse_port = clickhouse_connection.port
    clickhouse_username = clickhouse_connection.login
    clickhouse_password = clickhouse_connection.password
    clickhouse_database = 'bronze'
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_host,
        port=clickhouse_port,
        username=clickhouse_username,
        password=clickhouse_password,
        database=clickhouse_database
    )
    # Connect to MongoDB
    mongo_conn_id = kwargs['mongo_conn_id']
    mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
    mongo_db = mongo_hook.get_conn()['utube']
    mongo_videos_collection = mongo_db['videos']
    documents = list(mongo_videos_collection.find().limit(2))
    # Prepare data for ClickHouse insertion
    data_to_insert = []
    for doc in documents:
        data_to_insert.append((
            doc['_id'],
            doc['object']['owner_username'],
            doc['object']['owner_id'],
            doc['object']['title'],
            doc['object']['uid'],
            doc['object']['visit_count'],
            doc['object']['owner_name'],
            doc['object']['poster'],
            datetime.fromisoformat(doc['object']['posted_date']),
            doc['object']['posted_timestamp'],
            datetime.fromisoformat(doc['object']['sdate_rss']),
            doc['object']['sdate_rss_tp'],
            doc['object']['comments'],
            doc['object']['description'],
            doc['object']['is_deleted'],
            datetime.fromisoformat(doc['created_at']),
            datetime.fromisoformat(doc['expire_at']),
            doc.get('is_produce_to_kafka', False),
            doc.get('update_count', 0),
            doc['object']
        ))
    # Prepare column names for insertion
    column_names = [
        'id', 'owner_username', 'owner_id', 'title', 'uid', 'visit_count',
        'owner_name', 'poster', 'posted_date', 'posted_timestamp', 'sdate_rss',
        'sdate_rss_tp', 'comments', 'description', 'is_deleted', 'created_at',
        'expire_at', 'is_produce_to_kafka', 'update_count', '_raw_object'
    ]
    # Execute the insert query for each set of values
    clickhouse_client.insert(
        'videos_test_2',
        data_to_insert,
        column_names=column_names
    )
    return data_to_insert


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
