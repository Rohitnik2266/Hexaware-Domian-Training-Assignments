from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def simulate_work():
    time.sleep(15)

with DAG("retry_timeout_dag", start_date=datetime(2023,1,1),
         schedule_interval="@once", catchup=False) as dag:

    task = PythonOperator(
        task_id="long_task",
        python_callable=simulate_work,
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(seconds=20)
    )
