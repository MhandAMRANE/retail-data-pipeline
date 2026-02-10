from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data_engineer",
    "retries": 2
}

with DAG(
    dag_id="retail_dwh_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    clean_data = BashOperator(
        task_id="clean_and_load_data",
        bash_command="python /app/scripts/clean_data.py"
    )

    dim_product = BashOperator(
        task_id="build_dim_product",
        bash_command="psql -h postgres-server -U postgres -d retail_db -f /app/sql/create_dim_product.sql"
    )

    dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command="psql -h postgres-server -U postgres -d retail_db -f /app/sql/create_dim_date.sql"
    )

    dim_country = BashOperator(
        task_id="build_dim_country",
        bash_command="psql -h postgres-server -U postgres -d retail_db -f /app/sql/create_dim_country.sql"
    )

    fact_sales = BashOperator(
        task_id="build_fact_sales",
        bash_command="psql -h postgres-server -U postgres -d retail_db -f /app/sql/create_fact_sales.sql"
    )

    validate_dwh = BashOperator(
        task_id="validate_dwh_readiness",
        bash_command="python /app/scripts/validate_dwh.py"
    )

    clean_data >> [dim_product, dim_date, dim_country] >> fact_sales >> validate_dwh
