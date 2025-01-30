import json
import os
import datetime

import boto3
import pandas as pd
import numpy as np
import pymongo
from airflow import DAG
from airflow.hooks.base import BaseHook
from pymongo import InsertOne
from psycopg2.extras import execute_values
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

COLUMN_MAPPING = {
    '_id': 'id',
    'object_username': 'username',
    'object_userid': 'userid',
    'object_avatar_thumbnail': 'avatar_thumbnail',
    'object_is_official': 'is_official',
    'object_name': 'name',
    'object_bio_links': 'bio_links',
    'object_total_video_visit': 'total_video_visit',
    'object_video_count': 'video_count',
    'object_start_date': 'start_date',
    'object_start_date_timestamp': 'start_date_timestamp',
    'object_followers_count': 'followers_count',
    'object_following_count': 'following_count',
    'object_is_deleted': 'is_deleted',
    'object_country': 'country',
    'object_platform': 'platform',
    'created_at': 'created_at',
    'updated_at': 'updated_at',
    'update_count': 'update_count',
}

def download_data_from_s3(execution_date, **kwargs):
    conn = BaseHook.get_connection('arvan_s3_conn')
    access_key = conn.login
    secret_key = conn.password
    extra = json.loads(conn.extra or '{}')
    endpoint_url = extra.get('endpoint_url')
    s3_resource = boto3.resource(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    bucket_name = 'qbc'
    bucket = s3_resource.Bucket(bucket_name)
    execution_date = str(execution_date.date())

    for obj in bucket.objects.filter(Prefix=execution_date):
        object_name = obj.key
        dir_name = 'channels' if 'channels' in object_name else 'videos'
        download_path = f'/tmp/{dir_name}/{object_name.split("/")[-1]}'
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        bucket.download_file(object_name, download_path)
        print(f"Downloaded {object_name} to {download_path}")


def load_csv_to_postgres(execution_date, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='oltp_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TEMP TABLE temp_import AS SELECT * FROM test14 LIMIT 0
    """)
    print("Temporary table created successfully")

    csv_dir = '/tmp/channels'
    execution_date = str(execution_date.date())
    processed_files = 0
    total_rows = 0
    for file in os.listdir(csv_dir):
        if not (file.endswith('.csv') and execution_date in file):
            print(f"Skipping non-relevant file: {file}")
            continue

        file_path = os.path.join(csv_dir, file)
        df = pd.read_csv(
            file_path,
            dtype={
                'is_official': 'boolean',
                'video_count': 'Int64',
                'followers_count': 'Int64',
                'following_count': 'Int64',
                'total_video_visit': 'Int64',
                'start_date_timestamp': 'Int64'
            },
            na_values=['', 'null', 'NaN', 'NA', 'None', 'none'],
            keep_default_na=False
        )

        df.rename(columns=COLUMN_MAPPING, inplace=True)
        df = df.replace([np.nan, pd.NA], None)

        if 'is_official' in df.columns:
            df['is_official'] = df['is_official'].where(
                df['is_official'].notna(),
                None
            )

        db_columns = COLUMN_MAPPING.values()

        for col in db_columns:
            if col not in df.columns:
                df[col] = None

        df = df[db_columns]

        tuples = [tuple(x if x is not pd.NA else None for x in row) for row in df.to_numpy()]

        if tuples:
            query = f"""
                INSERT INTO temp_import ({','.join(db_columns)}) VALUES %s
            """
            execute_values(cursor, query, tuples, page_size=1000)
            total_rows += len(tuples)
            processed_files += 1
            conn.commit()
        else:
            print(f"No data to insert for {file}")

    if total_rows > 0:
        print("Starting deduplication process")
        insert_query = f"""
            INSERT INTO test14 ({','.join(db_columns)})
            SELECT {','.join(db_columns)}
            FROM temp_import
            WHERE NOT EXISTS (
                SELECT 1 FROM test14
                WHERE {' AND '.join([f'test14.{c} = temp_import.{c}' for c in db_columns])}
            )
        """
        cursor.execute(insert_query)
        conn.commit()
        inserted_rows = cursor.rowcount
        print(f"Successfully inserted {inserted_rows} new records")
    else:
        print("No data processed from all files")

    cursor.close()
    conn.close()


def load_json_to_mongo(execution_date, **kwargs):
    mongo_hook = MongoHook(mongo_conn_id='oltp_mongo_conn')
    client = mongo_hook.get_conn()
    db = client['utube']
    collection = db['videos']
    json_folder_path = '/tmp/videos'

    last_doc = collection.find_one(sort=[('offset', -1)])
    current_offset = last_doc['offset'] if last_doc else 0

    files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
    execution_date = str(execution_date.date())

    for file in files:
        if execution_date not in file:
            print(f"Skipping {file}")
            continue

        file_path = os.path.join(json_folder_path, file)
        with open(file_path, 'r') as f:
            data = [json.loads(line) for line in f]

        batch_size = 5000
        total_docs = len(data)

        for i in range(0, total_docs, batch_size):
            batch = data[i:i + batch_size]
            operations = []

            batch_original_ids = [doc['_id'].split('_')[0] for doc in batch]

            existing_docs = collection.find(
                {'original_id': {'$in': batch_original_ids}},
                {'original_id': 1, 'update_count': 1}
            )

            existing_map = {
                doc['original_id']: doc['update_count']
                for doc in existing_docs
            }

            for doc in batch:
                original_id = doc['_id']
                new_update_count = doc.get('update_count', 0)

                if original_id in existing_map:
                    existing_count = existing_map[original_id]
                    if new_update_count <= existing_count:
                        print(f"Skipping duplicate: {original_id}")
                        continue

                current_offset += 1
                operations.append(InsertOne({
                    **doc,
                    '_id': f"{original_id}_{current_offset}",
                    'original_id': original_id,
                    'offset': current_offset
                }))

            if operations:
                collection.bulk_write(operations, ordered=False)

            print(f"Processed {len(operations)}/{len(batch)} docs - {file}")

        print(f"Completed {file} with {total_docs} documents")


with DAG(
        dag_id='load_data_from_s3_to_postgres_and_mongo',
        schedule_interval='44 15 * * *',
        start_date=datetime.datetime(2025, 1, 13),
        catchup=True
) as dag:
    download_data_from_s3 = PythonOperator(
        task_id='download_data_from_s3',
        python_callable=download_data_from_s3,
        provide_context=True
    )

    load_csv_to_postgres = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
        provide_context=True
    )

    load_json_to_mongo = PythonOperator(
        task_id='load_json_to_mongo',
        python_callable=load_json_to_mongo,
        provide_context=True
    )

    download_data_from_s3 >> load_csv_to_postgres >> load_json_to_mongo