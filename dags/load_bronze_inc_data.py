from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
import pendulum
import datetime
import requests
import json


# Function to send messages to Telegram
def send_telegram_message(text,chat_id,proxy,bot_token):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to send message to Telegram: {response.text}")
def alert(context):
    send_telegram_message(
        text=f"""\U0001F534 Dag (( {context['dag'].dag_id} )) is failed.\U0001F534""",
        chat_id=Variable.get("dag_alerting_chat_id "),
        proxy=Variable.get("telegram_proxy"),
        bot_token=Variable.get("dag_alerting_telegram_bot_token"))

# Incremental extraction marker management
def get_pg_last_execution():
    return Variable.get("etl_pg_last_execution")

def update_pg_last_execution(offset):
    Variable.set("etl_pg_last_execution", offset)

def get_mg_last_execution():
    return Variable.get("etl_mg_last_execution")

def update_mg_last_execution(offset):
    Variable.set("etl_mg_last_execution", offset)


# ETL tasks
def etl_postgres(**kwargs):
    pg_last_execution = get_pg_last_execution()
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_client = Client(host=clickhouse_connection.host,
                                port=clickhouse_connection.port,
                                user=clickhouse_connection.login,
                                password=clickhouse_connection.password,
                                database='bronze'
                                )
    pg_conn_id = kwargs['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    pg_connection = pg_hook.get_connection(pg_conn_id)
    pg_host = pg_connection.host + ":" + pg_connection.port,
    pg_user = pg_connection.login,
    pg_password = pg_connection.password
    clickhouse_client.query(f"INSERT INTO channels_test SELECT * FROM postgresql({pg_host}, 'utube', 'channels', {pg_user}, {pg_password}) WHERE offset > {pg_last_execution}")

    # Update the last exec
    pg_latest_execution = clickhouse_client.query("select max(offset) from channels_test")
    update_pg_last_execution(pg_latest_execution)


def etl_mongo(**kwargs):
    mg_last_execution = get_mg_last_execution()
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_client = Client(host=clickhouse_connection.host,
                                port=clickhouse_connection.port,
                                user=clickhouse_connection.login,
                                password=clickhouse_connection.password,
                                database='bronze'
                                )
    mongo_conn_id = kwargs['mongo_conn_id']
    mongo_hook = MongoHook(conn_id=mongo_conn_id)
    collection = mongo_hook.get_collection("videos")
    query = {"offset": {"$gt": mg_last_execution}}
    documents = list(collection.find(query))

    clickhouse_videos_column_names = [
        'id',
        'owner_username',
        'owner_id',
        'title',
        'tags',
        'uid',
        'visit_count',
        'owner_name',
        'poster',
        'owner_avatar',
        'duration',
        'posted_date',
        'posted_timestamp',
        'sdate_rss',
        'sdate_rss_tp',
        'comments',
        'frame',
        'like_count',
        'description',
        'is_deleted',
        'created_at',
        'expire_at',
        'is_produce_to_kafka',
        'update_count',
        '_ingestion_ts',
        '_source',
        '_raw_object',
        'offset',
    ]

    data_to_insert = []
    for doc in documents:
        data_to_insert.append((
            doc['_id'],
            doc['object']['owner_username'],
            doc['object']['owner_id'],
            doc['object']['title'],
            doc['object']['tags'],
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
            doc['object']['_ingestion_ts'],
            doc['object']['_source'],
            json.dumps(doc['object']),
            doc['offset'],
        ))
    # Execute the insert query for each set of values
    clickhouse_client.insert(
        'videos_test',
        data_to_insert,
        column_names=clickhouse_videos_column_names
    )
    # Update the last exec
    mg_latest_execution=clickhouse_client.query("select max(offset) from videos_test")
    update_mg_last_execution(mg_latest_execution)



# Define DAG
with DAG(
        "load_bronze_inc_data",
        description="Incrementally ETL data from Postgres and MongoDB into ClickHouse",
        schedule_interval="0 20 * * *",
        start_date=pendulum.datetime(2025, 1, 7, tz="Asia/Tehran"),
        catchup=False,
        on_failure_callback=alert
) as dag:
    etl_postgres_task = PythonOperator(
        task_id="etl_postgres_task",
        python_callable=etl_postgres,
        provide_context=True,
    )

    etl_mongo_task = PythonOperator(
        task_id="etl_mongo_task",
        python_callable=etl_mongo,
        provide_context=True,
    )

    # Task dependencies
    [etl_postgres_task, etl_mongo_task]
