from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def parse_response(response_text):
    data = json.loads(response_text)
    print("Parsed:", data)

with DAG("api_interaction", start_date=datetime(2023,1,1), schedule_interval="@once", catchup=False) as dag:

    get_data = SimpleHttpOperator(
        task_id='get_crypto',
        method='GET',
        http_conn_id='crypto_api',  # Configure in Airflow UI
        endpoint='/v1/bpi/currentprice.json',
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        do_xcom_push=True
    )

    parse = PythonOperator(
        task_id="parse_response",
        python_callable=lambda **ctx: parse_response(ctx['ti'].xcom_pull(task_ids='get_crypto')),
        provide_context=True
    )

    get_data >> parse
