from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

def fail_me():
    raise Exception("Intentional fail")

def success():
    print("Success!")

with DAG("email_notify", start_date=datetime(2023,1,1), schedule_interval="@once", catchup=False) as dag:

    task1 = PythonOperator(task_id="try_fail", python_callable=fail_me, retries=0)

    task2 = PythonOperator(task_id="final_success", python_callable=success)

    failure_email = EmailOperator(
        task_id='email_on_failure',
        to='{{ var.value.alert_email }}',
        subject='Airflow Task Failed',
        html_content='A task has failed.',
        trigger_rule='one_failed'
    )

    success_email = EmailOperator(
        task_id='email_on_success',
        to='{{ var.value.alert_email }}',
        subject='Airflow Success',
        html_content='All tasks completed!',
        trigger_rule='all_success'
    )

    [task1, task2] >> [failure_email, success_email]
