import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago



def test_clickhouse():
    client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, username='', password='', database='bronze')

    data = client.command('show databases')

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
