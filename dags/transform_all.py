from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transform.tourist_arrivals import transform_tourist_arrivals
from transform.weather import transform_weather
from transform.flights import transform_flights
from transform.search_trends import transform_search_trends

default_args = {
    "owner": "atlasiq",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="transform_all",
    description="Transform all raw data into clean processed tables",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["transform", "morocco"],
) as dag:

    t_arrivals = PythonOperator(
        task_id="transform_tourist_arrivals",
        python_callable=transform_tourist_arrivals,
    )

    t_weather = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    t_flights = PythonOperator(
        task_id="transform_flights",
        python_callable=transform_flights,
    )

    t_trends = PythonOperator(
        task_id="transform_search_trends",
        python_callable=transform_search_trends,
    )

    # All transformations run in parallel — they are independent
    [t_arrivals, t_weather, t_flights, t_trends]
