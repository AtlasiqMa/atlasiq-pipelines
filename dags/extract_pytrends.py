from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract.pytrends import extract_google_trends

default_args = {
    "owner": "atlasiq",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="extract_pytrends",
    description="Extract Google Trends search interest for Morocco tourism keywords",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["extract", "pytrends", "google", "morocco"],
) as dag:

    extract_trends = PythonOperator(
        task_id="extract_google_trends",
        python_callable=extract_google_trends,
    )

    extract_trends