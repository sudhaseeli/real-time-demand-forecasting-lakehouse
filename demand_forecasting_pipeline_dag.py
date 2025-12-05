from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sudha",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="demand_forecasting_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_events = BashOperator(
        task_id="generate_stream_events",
        bash_command="python /opt/airflow/dags/../streaming/kafka_producers/simulate_orders.py",
    )

    batch_ingestion = BashOperator(
        task_id="batch_ingestion",
        bash_command="python /opt/airflow/dags/../spark_jobs/batch_ingestion.py",
    )

    feature_engineering = BashOperator(
        task_id="feature_engineering",
        bash_command="python /opt/airflow/dags/../spark_jobs/feature_engineering.py",
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/dags/../ml/train_model.py",
    )

    generate_forecasts = BashOperator(
        task_id="generate_forecasts",
        bash_command="python /opt/airflow/dags/../ml/generate_forecasts.py",
    )

    generate_events >> batch_ingestion >> feature_engineering >> train_model >> generate_forecasts
