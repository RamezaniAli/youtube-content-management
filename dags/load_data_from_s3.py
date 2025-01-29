import json
import os
import datetime
import time

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook


def download_data_from_s3(execution_date, **kwargs):
    s3_resource = boto3.resource(
        's3',
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
    postgres_hook = PostgresHook(postgres_conn_id='oltp_postgres_conn')
    conn = postgres_hook.get_sqlalchemy_engine()
    csv_folder_path = '/tmp/channels'

    csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    execution_date = str(execution_date.date())
    for csv_file in csv_files:
        if execution_date not in csv_file:
            print(f"Skipping {csv_file}")
            continue
        csv_path = os.path.join(csv_folder_path, csv_file)

        df = pd.read_csv(csv_path)

        df.to_sql(
            table_name,
            con=conn,
            if_exists="append",
            index=False
        )

        print(f"Loaded {csv_file} to {table_name}")

        time.sleep(1)


def load_json_to_mongo(execution_date, **kwargs):
    mongo_hook = MongoHook(mongo_conn_id='oltp_mongo_conn')
    client = mongo_hook.get_conn()
    db_name = 'utube'
    db = client[db_name]
    collection = db[collection_name]
    json_folder_path = '/tmp/videos'

    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)

    files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
    execution_date = str(execution_date.date())
    for file in files:
        if execution_date not in file:
            print(f"Skipping {file}")
            continue

        file_path = os.path.join(json_folder_path, file)
        data = [json.loads(line) for line in open(file_path, 'r')]
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]

            existing_ids = [doc['_id'] for doc in batch if collection.find_one({'_id': doc['_id']})]

            batch = [doc for doc in batch if doc['_id'] not in existing_ids]

            if batch:
                collection.insert_many(batch, ordered=False)

            print(f"Loaded batch of size {len(batch)} from {file} to {collection_name}")

        print(f"Loaded {file} to {collection_name}")


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
        python_callable=load_csv_to_postgres,
        provide_context=True
    )

    load_json_to_mongo = PythonOperator(
        task_id='load_json_to_mongo',
        python_callable=load_json_to_mongo,
        provide_context=True
    )

    download_data_from_s3 >> load_csv_to_postgres >> load_json_to_mongo