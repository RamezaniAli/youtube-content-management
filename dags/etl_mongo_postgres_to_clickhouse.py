import psutil
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from clickhouse_connect import get_client
from airflow.operators.python import PythonOperator
from datetime import datetime

from pendulum import duration


def get_dynamic_batch_size(max_batch_size=1000):
    memory_available = psutil.virtual_memory().available
    if memory_available < 500 * 1024 * 1024:
        return max_batch_size // 2
    return max_batch_size


def extract_from_postgres(batch_size=1000):
    postgres_hook = PostgresHook('oltp_postgres_conn')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        offset = 0
        while True:
            query = "SELECT * FROM channels LIMIT %s OFFSET %s;"
            cursor.execute(query, (batch_size, offset))
            records = cursor.fetchall()

            if not records:
                break

            yield records
            offset += batch_size
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def extract_from_mongo(batch_size=1000):
    mongo_hook = MongoHook('oltp_mongo_conn')
    mongo_client = mongo_hook.get_conn()

    try:
        db = mongo_client['utube']
        collection = db['videos']
        cursor = collection.find().batch_size(batch_size)
        batch = []

        for document in cursor:
            doc = {
                "id": document["_id"],
                "owner_username": document["object"].get("owner_username"),
                "owner_id": document["object"].get("owner_id"),
                "title": document["object"].get("title"),
                "tags": document["object"].get("tags"),
                "uid": document["object"].get("uid"),
                "visit_count": document["object"].get("visit_count"),
                "owner_name": document["object"].get("owner_name"),
                "poster": document["object"].get("poster"),
                "owener_avatar": document["object"].get("owener_avatar"),
                "duration": document["object"].get("duration"),
                "posted_date": document["object"].get("posted_date"),
                "posted_timestamp": document["object"].get("posted_timestamp"),
                "sdate_rss": document["object"].get("sdate_rss"),
                "sdate_rss_tp": document["object"].get("sdate_rss_tp"),
                "comments": document["object"].get("comments"),
                "frame": document["object"].get("frame"),
                "like_count": document["object"].get("like_count"),
                "description": document["object"].get("description"),
                "is_deleted": document["object"].get("is_deleted"),
                "created_at": document.get("created_at"),
                "expire_at": document.get("expire_at"),
                "is_produce_to_kafka": document.get("is_produce_to_kafka", False),
                "update_count": document.get("update_count", 0),
                "object": document.get("object")
            }
            batch.append(doc)

            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        mongo_client.close()


def create_bronze_schema():
    try:
        client = get_client(host='127.0.0.1', port=8123)
        client.command("CREATE DATABASE IF NOT EXISTS bronze")

        create_channels_table_query = """
            CREATE TABLE IF NOT EXISTS bronze.channels
            (
                id                      String,  
                username                String,  
                userid                  String,  
                avatar_thumbnail        String, 
                is_official             Nullable(UInt8), 
                name                    String,  
                bio_links               String,  
                total_video_visit       Int64,  
                video_count             Int32,  
                start_date              DateTime('UTC'),  
                start_date_timestamp    Int64,  
                followers_count         Nullable(Int64),  
                following_count         Nullable(Int64), 
                country                 LowCardinality(String), 
                platform                LowCardinality(String), 
                created_at              Nullable(DateTime('UTC')),  
                update_count            Int32,
                _source                 String DEFAULT 'postgres',
                _ingestion_ts           DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (userid, created_at);
        """
        client.command(create_channels_table_query)

        create_videos_table_query = """
            CREATE TABLE IF NOT EXISTS bronze.videos
            (
                id                  UInt64,
                owner_username      String,
                owner_id            String,
                title               String,
                tags                Nullable(String),
                uid                 String,
                visit_count         UInt64,
                owner_name          String,
                poster              String,
                owener_avatar       Nullable(String),
                duration            Nullable(UInt64),
                posted_date         DateTime('UTC'),
                posted_timestamp    UInt64,
                sdate_rss           DateTime('UTC'),
                sdate_rss_tp        UInt64,
                comments            Nullable(String),
                frame               Nullable(String),
                like_count          Nullable(UInt64),
                description         Nullable(String),
                is_deleted          UInt8,
                created_at          DateTime('UTC'),
                expire_at           DateTime('UTC'),
                is_produce_to_kafka UInt8,
                update_count        UInt32,

                _ingestion_ts       DateTime('UTC') DEFAULT now(),
                _source             String DEFAULT 'mongo',
                _raw_object         String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)     
            ORDER BY (owner_id, created_at);
        """
        client.command(create_videos_table_query)
    except Exception as e:
        print(f"An error occurred: {e}")

def load_to_clickhouse(postgres_batches, mongo_batches):
    client = get_client(host='127.0.0.1', port=8123)

    for batch in postgres_batches:
        client.insert('bronze.channels', batch)

    for batch in mongo_batches:
        client.insert('bronze.videos', batch)

    print("Data inserted successfully!")


default_args = {
    "owner": "Quera Team",
    "retries": 3,
    "retry_delay": duration(minutes=3)
}

dag = DAG(
        'etl_mongo_postgres_to_clickhouse',
        default_args=default_args,
        description='ETL with Dynamic Batch Size',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        max_active_runs=1,
        catchup=False,
)

extract_postgres = PythonOperator(
    dag=dag,
    task_id='extract_postgres',
    python_callable=extract_from_postgres,
    op_kwargs={'batch_size': get_dynamic_batch_size()},
)

extract_mongo = PythonOperator(
    dag=dag,
    task_id='extract_mongo',
    python_callable=extract_from_mongo,
    op_kwargs={'batch_size': get_dynamic_batch_size()},
)

create_bronze_schema = PythonOperator(
    dag=dag,
    task_id='create_schema',
    python_callable=create_bronze_schema
)

load_clickhouse = PythonOperator(
    dag=dag,
    task_id='load_clickhouse',
    python_callable=load_to_clickhouse,
    op_kwargs={
        'postgres_batches': '{{ ti.xcom_pull(task_ids="extract_postgres") }}',
        'mongo_batches': '{{ ti.xcom_pull(task_ids="extract_mongo") }}',
    },
)

[extract_postgres, extract_mongo] >> create_bronze_schema >> load_clickhouse