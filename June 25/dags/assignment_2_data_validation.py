import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

REQUIRED_COLUMNS = ["order_id", "customer_id", "order_date"]

def validate_data():
    df = pd.read_csv("C:\Users\ROHIT_NIKAM\airflow\data\orders.csv")
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    if df[REQUIRED_COLUMNS].isnull().any().any():
        raise ValueError("Null values found in required fields")

def summarize():
    print("Validation passed. Proceeding to summarization...")

with DAG("data_quality_validation", start_date=datetime(2023,1,1),
         schedule_interval="@once", catchup=False) as dag:

    validate = PythonOperator(task_id="validate", python_callable=validate_data)
    summarize = PythonOperator(task_id="summarize", python_callable=summarize)

    validate >> summarize
