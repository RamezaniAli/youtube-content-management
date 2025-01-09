from pendulum import duration
from airflow import DAG
import datetime
from clickhouse_driver import Client
from airflow.decorators import task
from dotenv import load_dotenv
import os
import psycopg2
import pymongo

load_dotenv()


default_args = {
    "owner": "Quera Team",
    "start_date": datetime.datetime(2025, 1, 9),
    "retries": 3,
    "retry_delay": duration(minutes=1)
}



def read_sql_file(path):
    with open(path, 'r') as f:
        sql = f.read()
    return sql


def create_clickhouse_schema():
    client = Client(host='localhost', port=9000)
    ddl = read_sql_file('scripts/bronze_schema.sql')
    client.execute(ddl)


def reading_from_postgres():
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

    pg_conn = psycopg2.connect(
        database='utube',
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host='localhost',
        port='5432'
    )

    click_conn = Client(host='localhost', port=9000)

    pg_cursor = pg_conn.cursor()
    BATCH_SIZE = 1000
    OFFSET = 0
    while True:
        # read data
        rows = pg_cursor.execute(f'SELECT * FROM channel LIMIT {BATCH_SIZE} OFFSET {OFFSET}').fetchall()
        if not rows:
            break
        # insert data 
        click_conn.execute(
            'INSERT INTO bronze.channel VALUES', rows
        )

        OFFSET += BATCH_SIZE

    pg_cursor.close()
    pg_conn.close()


def reading_from_mongodb():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['youtube']
    collection = db['videos']

    click_conn = Client(host='localhost', port=9000)

    for doc in collection.find():
        data = doc["object"]
        created_at = doc["created_at"]
        expire_at = doc["expire_at"]
        is_produce_to_kafka = doc["is_produce_to_kafka"]
        update_count = doc["update_count"]

        data + [created_at, expire_at, is_produce_to_kafka, update_count]

        fields_to_delete = ("_", "platform")
        for field in fields_to_delete:
            data.pop(field, None)
        
        click_conn.execute(
            'INSERT INTO bronze.video VALUES', data
        )


with DAG(dag_id='Bronze_layer_dag', max_active_runs=5, default_args=default_args, schedule_interval=None) as dag:

    ### clickhouse schema
    # 1. create database
    # 2. create table
    @task(queue="bronze_layer_queue")
    def create_bronze(**kwargs):

        create_clickhouse_schema()

    ### read from postgres as batches
    @task(queue="bronze_layer_queue")
    def read_from_postgres(**kwargs):
        reading_from_postgres()

    ### read from mongodb as batches
    @task(queue="bronze_layer_queue")
    def read_from_mongodb(**kwargs):
        reading_from_postgres
    

