import clickhouse_connect
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


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
        return 'final_task'


def etl_data_from_postgres(**kwargs):
    # Connect to Clickhouse
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
    clickhouse_channels_column_names = [
        'id', 'username', 'userid', 'avatar_thumbnail', 'is_official',
        'name', 'bio_links', 'total_video_visit', 'video_count', 'start_date',
        'start_date_timestamp', 'followers_count', 'following_count',
        'is_deleted', 'country', 'platform', 'created_at', 'updated_at',
        'update_count', 'offset'
    ]
    # Connect to PostgreSQL
    pg_conn_id = kwargs['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    last_processed_timestamp = '1970-01-01 00:00:00'
    batch_size = 200
    batch_number = 1
    while True:
        sql_query = f"SELECT * FROM channels WHERE start_date_timestamp > '{last_processed_timestamp}' LIMIT {batch_size}"
        records = pg_hook.get_records(sql_query)
        if not records:
            break
        # Prepare data for ClickHouse insertion
        data_to_insert = []
        for record in records:
            data_to_insert.append((
                record[0],   # _id
                record[1],   # username
                record[2],   # userid
                record[3],   # avatar_thumbnail
                record[4],   # is_official
                record[5],   # name
                record[6],   # bio_links
                record[7],   # total_video_visit
                record[8],   # video_count
                record[9],   # start_date
                record[10],  # start_date_timestamp
                record[11],  # followers_count
                record[12],  # following_count
                1 if record[13] is True else 0,  # is_deleted
                record[14],  # country
                record[15],  # platform
                record[16],  # created_at
                record[17],  # updated_at
                record[18],  # update_count
                record[19],  # offset_val
            ))
        print('='*100)
        print('Batch Number:', batch_number)
        print(data_to_insert)
        print('='*100)
        # Execute the insert query
        clickhouse_client.insert(
            'channels',
            data_to_insert,
            column_names=clickhouse_channels_column_names
        )
        last_processed_timestamp = records[-1][10]
        batch_number += 1
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
        op_kwargs={
            'clickhouse_conn_id': 'wh_clickhouse_conn'
        }
    )

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

    final_task = DummyOperator(
        task_id='final_task',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    create_channels_schema_task >> count_channels_records_task >> branch_task
    branch_task >> etl_data_from_postgres_task >> final_task
    branch_task >> final_task
