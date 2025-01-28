import clickhouse_connect
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


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
    # Connect to PostgreSQL
    pg_conn_id = kwargs['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    last_processed_timestamp = 0
    batch_size = 10
    batch_number = 1
    skip = 0
    while True:
        sql_query = f"""
        SELECT * FROM channels
        WHERE start_date_timestamp > {last_processed_timestamp}
        ORDER BY start_date_timestamp ASC
        LIMIT {batch_size}
        OFFSET {skip}
        """
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
        skip += batch_size
        batch_number += 1
    return 'Done!'


# DAG and its tasks
with DAG(
    dag_id='load_bronze_channels_data_test',
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
        task_id='final_task'
    )

    create_channels_schema_task >> etl_data_from_postgres_task >> final_task
