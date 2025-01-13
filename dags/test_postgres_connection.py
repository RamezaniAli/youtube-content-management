from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# Callbacks
def process_data(**kwargs):
    conn_id = kwargs['conn_id']
    hook = PostgresHook(postgres_conn_id=conn_id)
    sql_query = "SELECT * FROM channels;"
    records = hook.get_records(sql_query)
    kwargs['ti'].xcom_push(key='postgres_data', value=records)
    return records


# DAG and its tasks
with DAG(
    dag_id='postgres_record_count_dag',
    description='Count records in PostgreSQL table',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        provide_context=True,
        op_kwargs={'conn_id': 'oltp_postgres_conn'}
    )
