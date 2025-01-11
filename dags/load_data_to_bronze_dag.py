from pendulum import duration
from airflow import DAG
import datetime
from airflow.decorators import task
from dotenv import load_dotenv
from utils.bronze_database import BronzeIntegration

load_dotenv()

default_args = {
    "owner": "Quera Team",
    "start_date": datetime.datetime(2025, 1, 9),
    "retries": 3,
    "retry_delay": duration(minutes=1)
}

bronze_integration = BronzeIntegration()

with DAG(dag_id='Bronze_layer_dag', max_active_runs=5, default_args=default_args, schedule_interval=None) as dag:

    @task
    def create_clickhouse_schema():
        bronze_integration.create_clickhouse_schema()

    @task
    def read_from_postgres():
        bronze_integration.read_from_postgres()

    @task
    def read_from_mongodb():
        bronze_integration.read_from_mongo()
    
    schema_task = create_clickhouse_schema()
    postgres_task = read_from_postgres()
    mongo_task = read_from_mongodb()

    schema_task >> [postgres_task, mongo_task]
