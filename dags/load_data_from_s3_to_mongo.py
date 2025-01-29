import json
import os

import boto3
import pymongo
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago


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


def get_last_offset(collection):
    last_doc = collection.find_one(
        sort=[('offset', pymongo.DESCENDING)],
        projection={'offset': 1}
    )
    return last_doc['offset'] if last_doc else 0


def load_json_to_mongo(execution_date, **kwargs):
    # mongo_hook = MongoHook(mongo_conn_id='oltp_mongo_conn')
    # client = mongo_hook.get_conn()
    # db = client['utube']
    # collection_name = 'videos'
    # collection = db[collection_name]
    json_folder_path = '/tmp/videos'

    # current_offset = get_last_offset(collection)
    # new_offset = current_offset

    files = [f for f in os.listdir(json_folder_path) if f.endswith('.json')]
    execution_date = str(execution_date.date())

    print('#' * 50)
    print(f"Execution Date is: {execution_date}")
    for file in files:
        file_path = os.path.join(json_folder_path, file)
        data = [json.loads(line) for line in open(file_path, 'r')]
        print('#' * 50)
        print(file_path)
        print(data[0])
        print('#' * 50)
        # for document in data:
        #     print(document)
        # print('#' * 50)

    # for file in files:
    #     if execution_date not in file:
    #         print(f"Skipping {file}")
    #         continue

    #     file_path = os.path.join(json_folder_path, file)
    #     data = [json.loads(line) for line in open(file_path, 'r')]
    #     batch_size = 1000

    #     for i in range(0, len(data), batch_size):
    #         batch = data[i:i + batch_size]
    #         operations = []

    #         for doc in batch:
    #             composite_id = f"{doc['_id']}_{new_offset + 1}"

    #             new_offset += 1

    #             new_doc = {
    #                 **doc,
    #                 '_id': composite_id,
    #                 'offset': new_offset
    #             }

    #             operations.append(InsertOne(new_doc))

    #         if operations:
    #             collection.bulk_write(operations, ordered=False)

    #         print(f"Inserted {len(operations)} new documents from {file}")

    #     print(f"Processed {file}")


with DAG(
        dag_id='load_data_from_s3_to_mongo',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False
) as dag:
    download_data_from_s3 = PythonOperator(
        task_id='download_data_from_s3',
        python_callable=download_data_from_s3,
    )

    load_json_to_mongo = PythonOperator(
        task_id='load_json_to_mongo',
        python_callable=load_json_to_mongo,
    )

    download_data_from_s3 >> load_json_to_mongo
