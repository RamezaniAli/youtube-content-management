import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


# Callbacks
def count_channels_records(**kwargs):
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    clickhouse_connection = BaseHook.get_connection(clickhouse_conn_id)
    clickhouse_host = clickhouse_connection.host
    clickhouse_port = clickhouse_connection.port
    clickhouse_username = clickhouse_connection.login
    clickhouse_password = clickhouse_connection.password
    clickhouse_database = 'bronze'
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_host,
        port=clickhouse_port,
        username=clickhouse_username,
        password=clickhouse_password,
        database=clickhouse_database
    )
    result = clickhouse_client.query('SELECT COUNT(*) FROM channels')
    count = result.result_set[0][0]
    return count


def branch_based_on_count(**kwargs):
    count = kwargs['ti'].xcom_pull(task_ids='count_channels_records_task')
    if count == 0:
        return 'etl_data_from_postgres_task'
    else:
        return 'skip_etl_task'


def etl_data_from_postgres(**kwargs):
    print("ETL is running...")
    return 'ETL completed!'


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

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_based_on_count,
        provide_context=True,
    )

    etl_data_from_postgres_task = PythonOperator(
        task_id='etl_data_from_postgres_task',
        python_callable=etl_data_from_postgres,
        provide_context=True,
        op_kwargs={
            'postgres_conn_id': 'oltp_postgres_conn',
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

    skip_etl_task = DummyOperator(
        task_id='skip_etl_task'
    )

    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    count_channels_records_task >> branch_task >> [etl_data_from_postgres_task, skip_etl_task]
    etl_data_from_postgres_task >> dummy_task
    skip_etl_task >> dummy_task
