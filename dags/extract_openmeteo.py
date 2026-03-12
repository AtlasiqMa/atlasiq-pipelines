from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.openmeteo import extract_weather

default_args = {
    "owner": "atlasiq",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="extract_openmeteo",
    description="Extract weather forecast for Moroccan cities from Open-Meteo",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["extract", "openmeteo", "weather", "morocco"],
) as dag:

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    extract_weather_task