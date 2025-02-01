from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
import clickhouse_connect
import pendulum
from datetime import datetime
import requests
import json


# Function to send messages to Telegram
def send_telegram_message(text,chat_id,bot_token):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to send Telegram message: {response.text}")

def test_telegram_alert(context):
    chat_id = Variable.get("dag_alerting_chat_id")
    bot_token = Variable.get("dag_alerting_telegram_bot_token")
    text = f"""🚀 Dag (( {context['dag'].dag_id} )) is failed.🚀"""
    send_telegram_message(text, chat_id, bot_token)


# Incremental extraction marker management
def get_pg_last_execution():
    return Variable.get("etl_pg_last_execution")

def update_pg_last_execution(**kwargs):
    pg_latest_execution = kwargs['ti'].xcom_pull(task_ids='etl_postgres_task')
    Variable.set("etl_pg_last_execution", pg_latest_execution)

def get_mg_last_execution():
    return Variable.get("etl_mg_last_execution")

def update_mg_last_execution(**kwargs):
    mg_latest_execution = kwargs['ti'].xcom_pull(task_ids='etl_mongo_task')
    Variable.set("etl_mg_last_execution", mg_latest_execution)


# ETL tasks
def etl_postgres(**kwargs):
    pg_last_execution = kwargs['ti'].xcom_pull(task_ids='get_pg_last_execution_task')
    pg_last_execution = int(pg_last_execution)
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_client = clickhouse_connect.get_client(host=clickhouse_connection.host,
                                                      port=clickhouse_connection.port,
                                                      user=clickhouse_connection.login,
                                                      password=clickhouse_connection.password,
                                                      database='bronze'
                                                      )
    pg_conn_id = kwargs['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    sql_query = f"""
                SELECT * FROM channels WHERE offset_val > {pg_last_execution} 
                """
    records = pg_hook.get_records(sql_query)

    clickhouse_channels_column_names = [
        'id', 'username', 'userid', 'avatar_thumbnail', 'is_official', 'name', 'bio_links', 'total_video_visit',
        'video_count', 'start_date', 'start_date_timestamp', 'followers_count', 'following_count', 'is_deleted',
        'country', 'platform', 'created_at', 'updated_at', 'update_count', 'offset'
    ]

    data_to_insert = []
    for record in records:
        data_to_insert.append((
            record[0],  # _id
            record[1],  # username
            record[2],  # userid
            record[3],  # avatar_thumbnail
            record[4],  # is_official
            record[5],  # name
            record[6],  # bio_links
            record[7],  # total_video_visit
            record[8],  # video_count
            record[9],  # start_date
            record[10],  # start_date_timestamp
            record[11],  # followers_count
            record[12],  # following_count
            1 if record[13] is True else 0,  # is_deleted
            record[14],  # country
            record[15],  # platform
            record[16],  # created_at
            record[17],  # updated_at
            record[18],  # update_count
            record[19],  # offset_val
        ))
    # Execute the insert query
    clickhouse_client.insert(
        'channels',
        data_to_insert,
        column_names=clickhouse_channels_column_names
    )
    # Update the last exec
    pg_latest_execution = clickhouse_client.query('select max(offset) from channels')
    pg_latest_execution = pg_latest_execution.result_set[0][0]
    pg_latest_execution = pg_latest_execution if pg_latest_execution > pg_last_execution else pg_last_execution

    return pg_latest_execution


def etl_mongo(**kwargs):
    mg_last_execution = kwargs['ti'].xcom_pull(task_ids='get_mg_last_execution_task')
    mg_last_execution = int(mg_last_execution)
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_client = clickhouse_connect.get_client(host=clickhouse_connection.host,
                                                      port=clickhouse_connection.port,
                                                      user=clickhouse_connection.login,
                                                      password=clickhouse_connection.password,
                                                      database='bronze'
                                                      )
    mongo_conn_id = kwargs['mongo_conn_id']
    mongo_hook = MongoHook(mongo_conn_id)
    mongo_db = mongo_hook.get_conn()['utube']
    mongo_videos = mongo_db['videos']

    clickhouse_videos_column_names = [
        'id', 'owner_username', 'owner_id', 'title', 'tags', 'uid', 'visit_count', 'owner_name', 'poster',
        'owner_avatar', 'duration', 'posted_date', 'posted_timestamp', 'sdate_rss', 'sdate_rss_tp', 'comments',
        'frame', 'like_count', 'description', 'is_deleted', 'created_at', 'expire_at', 'is_produce_to_kafka',
        'update_count', '_raw_object', 'offset',
    ]
    batch_size = 500
    skip = 0
    while True:
        query = {"offset": {"$gt": mg_last_execution + skip}}
        documents = list(mongo_videos.find(query).limit(batch_size))
        if not documents:
            break
        data_to_insert = []
        for doc in documents:
            data_to_insert.append((
                doc.get('original_id', ''),  # Ensure key exists
                doc['object'].get('owner_username', ''),
                doc['object'].get('owner_id', ''),
                doc['object'].get('title', ''),
                doc['object'].get('tags', []),  # Ensure it's a list
                doc['object'].get('uid', ''),
                doc['object'].get('visit_count', 0),
                doc['object'].get('owner_name', ''),
                doc['object'].get('poster', ''),
                doc['object'].get('owner_avatar', ''),
                doc['object'].get('duration', 0),
                doc['object'].get('posted_date', "1970-01-01T00:00:00"),  # Handle missing/invalid dates
                doc['object'].get('posted_timestamp', 0),
                doc['object'].get('sdate_rss', "1970-01-01T00:00:00"),
                doc['object'].get('sdate_rss_tp', 0),
                doc['object'].get('comments', 0),
                doc['object'].get('frame', ''),
                doc['object'].get('like_count', 0),
                doc['object'].get('description', ''),
                doc['object'].get('is_deleted', False),
                doc.get('created_at', "1970-01-01T00:00:00"),
                doc.get('expire_at', "1970-01-01T00:00:00"),
                doc.get('is_produce_to_kafka', False),
                doc.get('update_count', 0),
                json.dumps(doc['object'], ensure_ascii=False),  # Ensure JSON encoding
                doc.get('offset', 0),
            ))
        # Execute the insert query for each set of values
        clickhouse_client.insert(
            'videos',
            data_to_insert,
            column_names=clickhouse_videos_column_names
        )
        skip += batch_size

    # Update the last exec
    mg_latest_execution = clickhouse_client.query('select max(offset) from videos')
    mg_latest_execution = mg_latest_execution.result_set[0][0]
    mg_latest_execution = mg_latest_execution if mg_latest_execution > mg_last_execution else mg_last_execution
    return mg_latest_execution


# Define DAG
with DAG(
        "load_bronze_inc_data",
        description="Incrementally ETL data from Postgres and MongoDB into ClickHouse",
        schedule_interval="0 21 * * *",
        start_date=pendulum.datetime(2025, 1, 25, tz="Asia/Tehran"),
        catchup=False,
) as dag:

    get_pg_last_execution_task = PythonOperator(
        task_id="get_pg_last_execution_task",
        python_callable=get_pg_last_execution,
        provide_context=True,
        on_failure_callback=test_telegram_alert
    )

    get_mg_last_execution_task = PythonOperator(
        task_id="get_mg_last_execution_task",
        python_callable=get_mg_last_execution,
        provide_context=True,
        on_failure_callback=test_telegram_alert
    )


    update_pg_last_execution_task = PythonOperator(
        task_id="update_pg_last_execution_task",
        python_callable=update_pg_last_execution,
        provide_context=True,
        on_failure_callback=test_telegram_alert
    )


    update_mg_last_execution_task = PythonOperator(
        task_id="update_mg_last_execution_task",
        python_callable=update_mg_last_execution,
        provide_context=True,
        on_failure_callback=test_telegram_alert
    )

    etl_postgres_task = PythonOperator(
        task_id="etl_postgres_task",
        python_callable=etl_postgres,
        provide_context=True,
        op_kwargs={
            'postgres_conn_id': 'oltp_postgres_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        },
        on_failure_callback=test_telegram_alert
    )

    etl_mongo_task = PythonOperator(
        task_id="etl_mongo_task",
        python_callable=etl_mongo,
        provide_context=True,
        op_kwargs={
            'mongo_conn_id': 'oltp_mongo_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        },
        on_failure_callback=test_telegram_alert
    )

    # Task dependencies
    get_pg_last_execution_task >> etl_postgres_task >> update_pg_last_execution_task
    get_mg_last_execution_task >> etl_mongo_task >> update_mg_last_execution_task
