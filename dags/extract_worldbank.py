from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.worldbank import extract_tourist_arrivals

default_args = {
    "owner": "atlasiq",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="extract_worldbank",
    description="Extract Morocco tourism statistics from World Bank API",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["extract", "worldbank", "morocco"],
) as dag:

    extract_arrivals = PythonOperator(
        task_id="extract_tourist_arrivals",
        python_callable=extract_tourist_arrivals,
    )

    extract_arrivals