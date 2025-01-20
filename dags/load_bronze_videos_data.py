import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import json


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


def count_videos_records(**kwargs):
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
    result = clickhouse_client.query('SELECT COUNT(*) FROM videos')
    count = result.result_set[0][0]
    return count


def branch_based_on_count(**kwargs):
    count = kwargs['ti'].xcom_pull(task_ids='count_videos_records')
    if count == 0:
        return 'etl_data_from_mongo_task'
    else:
        return 'final_task'


def etl_data_from_mongo(**kwargs):
    # Connect to Clickhouse
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_host = clickhouse_connection.host
    clickhouse_port = clickhouse_connection.port
    clickhouse_username = clickhouse_connection.login
    clickhouse_password = clickhouse_connection.password
    clickhouse_database = 'bronze'
    clickhouse_videos_column_names = [
        'id', 'owner_username', 'owner_id', 'title', 'uid', 'visit_count',
        'owner_name', 'poster', 'owner_avatar', 'duration', 'posted_date',
        'posted_timestamp', 'sdate_rss', 'sdate_rss_tp', 'comments', 'frame',
        'like_count', 'description', 'is_deleted', 'created_at', 'expire_at',
        'is_produce_to_kafka', 'update_count', '_raw_object'
    ]
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
    batch_size = 1000
    skip = 0
    batch_number = 1
    while True:
        documents = list(mongo_videos_collection.find().skip(skip).limit(batch_size))
        if not documents:
            break

        # Prepare data for ClickHouse insertion
        print('='*100)
        print('Batch Number:', batch_number)
        print('='*100)
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
                doc['object'].get('owner_avatar'),
                doc['object']['duration'],
                datetime.fromisoformat(doc['object']['posted_date']),
                doc['object']['posted_timestamp'],
                datetime.fromisoformat(doc['object']['sdate_rss']),
                doc['object']['sdate_rss_tp'],
                doc['object']['comments'],
                doc['object']['frame'],
                doc['object']['like_count'],
                doc['object']['description'],
                doc['object']['is_deleted'],
                doc['created_at'],
                doc['expire_at'],
                doc.get('is_produce_to_kafka', False),
                doc.get('update_count', 0),
                json.dumps(doc['object'])
            ))
        # Execute the insert query for each set of values
        clickhouse_client.insert(
            'videos',
            data_to_insert,
            column_names=clickhouse_videos_column_names
        )
        skip += batch_size
        batch_number += 1
    return 'Done!'


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

    count_videos_records_task = PythonOperator(
        task_id='count_videos_records_task',
        python_callable=count_videos_records,
        provide_context=True,
        op_kwargs={
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_based_on_count,
        provide_context=True,
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

    final_task = DummyOperator(
        task_id='final_task',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    create_videos_schema_task >> count_videos_records_task >> branch_task
    branch_task >> etl_data_from_mongo_task >> final_task
    branch_task >> final_task
