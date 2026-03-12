from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.opensky import extract_flight_arrivals

default_args = {
    "owner": "atlasiq",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="extract_opensky",
    description="Extract flight arrivals for Moroccan airports from OpenSky Network",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["extract", "opensky", "flights", "morocco"],
) as dag:

    extract_flights = PythonOperator(
        task_id="extract_flight_arrivals",
        python_callable=extract_flight_arrivals,
    )

    extract_flights
