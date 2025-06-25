from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG("parent_dag", start_date=datetime(2023,1,1), schedule_interval="@once", catchup=False) as dag:

    task1 = PythonOperator(
        task_id="parent_task",
        python_callable=lambda: print("Running parent DAG task")
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        conf={"date": str(datetime.now())}
    )

    task1 >> trigger
