from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Callbacks
def process_data_from_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    hook = MongoHook(mongo_conn_id=conn_id)
    db = hook.get_conn()['utube']
    videos_collection = db['videos']

    documents = list(videos_collection.find().limit(10))

    data = []
    for document in documents:
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
        data.append(doc)

    kwargs['ti'].xcom_push(key='mongo_data', value=data)

    return data


# DAG and its tasks
with DAG(
    dag_id='mongo_record_count_dag',
    description='Count records in MongoDB collection',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data_from_mongo_task',
        python_callable=process_data_from_mongo,
        op_kwargs={'conn_id': 'oltp_mongo_conn'}
    )
