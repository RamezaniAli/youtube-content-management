import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


# Callbacks
def process_data_task(**kwargs):
    conn_id = kwargs['conn_id']
    conn = BaseHook.get_connection(conn_id)
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
    query = 'SELECT COUNT(*) FROM channels'
    result = client.query(query)
    count = result.result_set[0][0]
    return f"Number of records in channels table: {count}"


# DAG and its tasks
with DAG(
    dag_id='clickhouse_record_count_dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data_task,
        op_kwargs={'conn_id': 'wh_clickhouse_conn'}
    )
