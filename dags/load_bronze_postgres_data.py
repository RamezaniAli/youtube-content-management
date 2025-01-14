import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    create_channels_table_query = """
        CREATE TABLE IF NOT EXISTS bronze.channels_test (
            id String,
            created_at DateTime64(3, 'UTC') DEFAULT toDateTime64(0, 3, 'UTC')
        )
        ENGINE = MergeTree
        PRIMARY KEY (id)
        ORDER BY (id);
    """
    # client.query(create_channels_table_query)
    return "Done!"


# DAG and its tasks
with DAG(
    dag_id='load_bronze_postgres_data',
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
