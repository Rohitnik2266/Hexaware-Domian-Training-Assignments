from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def child_task(**context):
    print("Triggered by parent")
    print("Metadata passed:", context["dag_run"].conf)

with DAG("child_dag", start_date=datetime(2023,1,1), schedule_interval=None, catchup=False) as dag:

    run = PythonOperator(
        task_id="child_task",
        python_callable=child_task,
        provide_context=True
    )
