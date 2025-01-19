import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


# Callbacks
def count_channels_records(**kwargs):
    clickhouse_conn_id = 'wh_clickhouse_conn'
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_host = clickhouse_connection.host
    # clickhouse_port = clickhouse_connection.port
    clickhouse_username = clickhouse_connection.login
    clickhouse_password = clickhouse_connection.password
    clickhouse_database = 'bronze'
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_host,
        port=8123,
        username=clickhouse_username,
        password=clickhouse_password,
        database=clickhouse_database
    )
    result = clickhouse_client.query('SELECT COUNT(*) FROM channels')
    count = result.result_set[0][0]
    return count


def etl_data_from_postgres(**kwargs):
    return 'Done!'


# DAG and its tasks
with DAG(
    dag_id='check_bronze_channels_data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    count_channels_records_task = PythonOperator(
        task_id='count_channels_records_task',
        python_callable=count_channels_records,
        provide_context=True,
        op_kwargs={
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

    etl_data_from_postgres_task = PythonOperator(
        task_id='etl_data_from_postgres_task',
        python_callable=etl_data_from_postgres
    )

    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    count_channels_records_task >> etl_data_from_postgres_task >> dummy_task
