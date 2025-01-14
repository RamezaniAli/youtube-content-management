import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


def test_clickhouse():
    conn = BaseHook.get_connection('wh_clickhouse_conn')
    host = conn.host
    port = conn.port
    username = conn.login
    password = conn.password
    # database = conn.schema
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database='bronze'
    )
    data = client.command('SHOW TABLES')
    return data


with DAG(
    dag_id='test_clickhouse',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='test_clickhouse',
        python_callable=test_clickhouse
    )
