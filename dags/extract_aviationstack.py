from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.aviationstack import extract_aviationstack_flights

default_args = {
    "owner": "atlasiq",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="extract_aviationstack",
    description="Extract flight arrivals and departures for Moroccan airports from AviationStack",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["extract", "aviationstack", "flights", "morocco"],
) as dag:

    extract_flights = PythonOperator(
        task_id="extract_aviationstack_flights",
        python_callable=extract_aviationstack_flights,
    )

    extract_flights