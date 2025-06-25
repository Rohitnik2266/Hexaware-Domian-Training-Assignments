from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import shutil

def process_file():
    print("Processing file...")

def move_file():
    shutil.move("C:\Users\ROHIT_NIKAM\airflow\data\incoming\report.csv", "C:\Users\ROHIT_NIKAM\airflow\data\archive\    report.csv")

with DAG("file_sensor_pipeline", start_date=datetime(2023,1,1),
         schedule_interval="@once", catchup=False) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_report",
        filepath="C:\Users\ROHIT_NIKAM\airflow\data\incoming\report.csv",
        fs_conn_id='fs_default',  
        timeout=600,  
        poke_interval=30,
        mode='poke'
    )

    process = PythonOperator(task_id="process_file", python_callable=process_file)
    move = PythonOperator(task_id="move_file", python_callable=move_file)

    wait_for_file >> process >> move
