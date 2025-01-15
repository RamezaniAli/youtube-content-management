import clickhouse_connect
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook


# Callbacks
def create_channels_schema(**kwargs):
    clickhouse_conn_id = kwargs['clickhouse_conn_id']
    conn = BaseHook.get_connection(clickhouse_conn_id)
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
    # Read SQL query file
    sql_file_path = '/opt/airflow/scripts/bronze_channels_schema.sql'
    with open(sql_file_path, 'r') as file:
        create_channels_table_query = file.read()
    # Execute the SQL query
    client.query(create_channels_table_query)
    return 'Done!'


def etl_data_from_postgres(**kwargs):
    # Connect to Clickhouse
    clickhouse_conn_id = kwargs['wh_clickhouse_conn']
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
    # Connect to PostgreSQL
    pg_conn_id = kwargs['oltp_postgres_conn']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    # Prepare data for ClickHouse insertion
    batch_size = 5
    skip = 0
    sql_query = f"SELECT * FROM your_table LIMIT {batch_size} OFFSET {skip}"
    records = pg_hook.get_records(sql_query)
    for record in records:
        print(record)
    # Execute the insert query
    return 'Done!'


# DAG and its tasks
with DAG(
    dag_id='load_bronze_channels_data',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    create_channels_schema_task = PythonOperator(
        task_id='create_channels_schema_task',
        python_callable=create_channels_schema,
        provide_context=True,
        op_kwargs={'clickhouse_conn_id': 'wh_clickhouse_conn'}
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

    dummy_task = DummyOperator(
        task_id='dummy_task'
    )

    create_channels_schema_task >> etl_data_from_postgres_task >> dummy_task
