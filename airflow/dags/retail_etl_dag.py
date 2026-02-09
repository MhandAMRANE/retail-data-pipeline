from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    clean_data = BashOperator(
        task_id="clean_and_load_data",
        bash_command="python /app/scripts/clean_data.py"
    )

    clean_data
