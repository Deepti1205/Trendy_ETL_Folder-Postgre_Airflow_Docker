from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime


import sys
sys.path.append("/opt/airflow/scripts")

from run_pipeline import run_etl

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # -------------------------
    # File Sensors
    # -------------------------
    wait_for_customers = FileSensor(
        task_id="wait_for_customers",
        filepath="data/incoming/customers.csv",
        poke_interval=5,
        timeout=300,
        mode="poke"
    )

    wait_for_products = FileSensor(
        task_id="wait_for_products",
        filepath="data/incoming/products.csv",
        poke_interval=5,
        timeout=300,
        mode="poke"
    )

    wait_for_transactions = FileSensor(
        task_id="wait_for_transactions",
        filepath="data/incoming/transactions.csv",
        poke_interval=5,
        timeout=300,
        mode="poke"
    )

    # -------------------------
    # Processing Tasks
    # -------------------------
    process_customers = PythonOperator(
        task_id="process_customers",
        python_callable=run_etl,
        op_args=["customers.csv"]
    )

    process_products = PythonOperator(
        task_id="process_products",
        python_callable=run_etl,
        op_args=["products.csv"]
    )

    process_transactions = PythonOperator(
        task_id="process_transactions",
        python_callable=run_etl,
        op_args=["transactions.csv"]
    )

    # -------------------------
    # Dependencies
    # -------------------------
    wait_for_customers >> process_customers  >>  wait_for_products >> process_products >> wait_for_transactions >> process_transactions