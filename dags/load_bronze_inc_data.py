from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
import clickhouse_connect
import pendulum
import datetime
import requests
import json


# Function to send messages to Telegram
def send_telegram_message(text,chat_id,bot_token):
    url = f"https://api.telegram.org/bot7513376273:AAGsUYkNj_G-EnkE9fyMdfQB-cz7o_TpN-0/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, json=payload)
def alert(context):
    send_telegram_message(
        text=f"""\U0001F534 Dag (( {context['dag'].dag_id} )) is failed.\U0001F534""",
        chat_id=Variable.get("dag_alerting_chat_id"),
        bot_token=Variable.get("dag_alerting_telegram_bot_token"))

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

def print_context_info():
    context = get_current_context()


# ETL tasks
def etl_postgres(**kwargs):
    pg_last_execution = kwargs['ti'].xcom_pull(task_ids='get_pg_last_execution_task')
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
        'id',
        'username',
        'userid',
        'avatar_thumbnail',
        'is_official',
        'name',
        'bio_links',
        'total_video_visit',
        'video_count',
        'start_date',
        'start_date_timestamp',
        'followers_count',
        'following_count',
        'is_deleted',
        'country',
        'platform',
        'created_at',
        'updated_at',
        'update_count',
        'offset'
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
        'channels_test',
        data_to_insert,
        column_names=clickhouse_channels_column_names
    )
    # Update the last exec
    pg_latest_execution = clickhouse_client.query('select max(offset) from channels_test')
    pg_latest_execution = pg_latest_execution.result_set[0][0]
    return pg_latest_execution


def etl_mongo(**kwargs):
    mg_last_execution = kwargs['ti'].xcom_pull(task_ids='get_mg_last_execution_task')
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
    query = {"offset": {"$gt": '6328627'}}
    documents = list(mongo_videos.find(query))

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
    mg_latest_execution= clickhouse_client.query('select max(offset) from videos_test')
    mg_latest_execution = mg_latest_execution.result_set[0][0]
    return mg_latest_execution


# Define DAG
with DAG(
        "load_bronze_inc_data",
        description="Incrementally ETL data from Postgres and MongoDB into ClickHouse",
        schedule_interval="30 23 * * *",
        start_date=pendulum.datetime(2025, 1, 25, tz="Asia/Tehran"),
        catchup=False,
        on_failure_callback=alert
) as dag:

    get_pg_last_execution_task = PythonOperator(
        task_id="get_pg_last_execution_task",
        python_callable=get_pg_last_execution,
        provide_context=True,
    )

    get_mg_last_execution_task = PythonOperator(
        task_id="get_mg_last_execution_task",
        python_callable=get_mg_last_execution,
        provide_context=True,
    )


    update_pg_last_execution_task = PythonOperator(
        task_id="update_pg_last_execution_task",
        python_callable=update_pg_last_execution,
        provide_context=True,
    )


    update_mg_last_execution_task = PythonOperator(
        task_id="update_mg_last_execution_task",
        python_callable=update_mg_last_execution,
        provide_context=True,
    )

    etl_postgres_task = PythonOperator(
        task_id="etl_postgres_task",
        python_callable=etl_postgres,
        provide_context=True,
        op_kwargs={
            'postgres_conn_id': 'oltp_postgres_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

    etl_mongo_task = PythonOperator(
        task_id="etl_mongo_task",
        python_callable=etl_mongo,
        provide_context=True,
        op_kwargs={
            'mongo_conn_id': 'oltp_mongo_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )
    context_info_task = PythonOperator(
        task_id="context_info_task",
        python_callable=print_context_info,
        provide_context=True,
    )

    # Task dependencies
    context_info_task >> get_pg_last_execution_task >> etl_postgres_task >> update_pg_last_execution_task
    context_info_task >> get_mg_last_execution_task >> etl_mongo_task >> update_mg_last_execution_task
