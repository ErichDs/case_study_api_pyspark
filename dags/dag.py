from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.challenge_1.retrieve_store_coingecko import main
from src.challenge_2.scriptETL import main as m

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

challenge_one_dag = DAG(
    dag_id = "process_coingecko_data",
    default_args=default_args,
    description='DAG to retrieve data from Coingecko Public API and store its data local lake environment',
    schedule_interval=timedelta(days=1)
)

task_etl = PythonOperator(
    dag=challenge_one_dag,
    task_id='retrieve_store_coingecko_data',
    python_callable=main
)

# dependencies (I wrapped all etl in a same script, so there is no need at this time)
task_etl

challenge_two_dag = DAG(
    dag_id="process_tables",
    default_args=default_args,
    description="This dags consume tables from the case and writes as parquet",
    schedule_interval=timedelta(days=1)
)

task_challenge_two = PythonOperator(
    dag=challenge_two_dag,
    task_id="process_tables",
    python_callable=m
)

task_challenge_two