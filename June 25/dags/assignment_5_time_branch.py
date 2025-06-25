from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pendulum

def check_time():
    now = pendulum.now("Asia/Kolkata")
    if now.weekday() >= 5:
        return "skip_dag"
    elif now.hour < 12:
        return "task_morning"
    else:
        return "task_afternoon"

with DAG("time_based_branching", start_date=datetime(2023,1,1), schedule_interval="@daily", catchup=False) as dag:

    branch = BranchPythonOperator(task_id="time_check", python_callable=check_time)

    task_morning = DummyOperator(task_id="task_morning")
    task_afternoon = DummyOperator(task_id="task_afternoon")
    skip_dag = DummyOperator(task_id="skip_dag")

    cleanup = DummyOperator(task_id="cleanup", trigger_rule="none_failed_min_one_success")

    branch >> [task_morning, task_afternoon, skip_dag] >> cleanup
