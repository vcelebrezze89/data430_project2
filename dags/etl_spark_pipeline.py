from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "tori",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_spark_pipeline",
    default_args=default_args,
    description="Project 2 ETL pipeline with Airflow, Spark, and PostgreSQL",
    schedule=None,
    start_date=datetime(2026, 3, 24),
    catchup=False,
    tags=["project2", "spark", "etl"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/generate_data.py",
    )

    validate_files = BashOperator(
        task_id="validate_files",
        bash_command="ls -lah /opt/airflow/data/raw",
    )

    spark_cleanse_customers = BashOperator(
        task_id="spark_cleanse_customers",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/spark_cleanse.py customers /opt/spark/data/raw/customers.csv /opt/spark/data/cleaned/customers",
    )

    spark_cleanse_suppliers = BashOperator(
        task_id="spark_cleanse_suppliers",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/spark_cleanse.py suppliers /opt/spark/data/raw/suppliers.csv /opt/spark/data/cleaned/suppliers",
    )

    spark_cleanse_products = BashOperator(
        task_id="spark_cleanse_products",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/spark_cleanse.py products /opt/spark/data/raw/products.csv /opt/spark/data/cleaned/products",
    )

    spark_cleanse_daily_sales = BashOperator(
        task_id="spark_cleanse_daily_sales",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/spark_cleanse.py daily_sales /opt/spark/data/raw/daily_sales.csv /opt/spark/data/cleaned/daily_sales",
    )

    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit /opt/spark/jobs/spark_analytics.py",
    )

    generate_data >> validate_files
    validate_files >> [
        spark_cleanse_customers,
        spark_cleanse_suppliers,
        spark_cleanse_products,
        spark_cleanse_daily_sales,
    ]
    [
        spark_cleanse_customers,
        spark_cleanse_suppliers,
        spark_cleanse_products,
        spark_cleanse_daily_sales,
    ] >> spark_analytics