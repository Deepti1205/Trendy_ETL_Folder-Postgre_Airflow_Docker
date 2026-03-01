from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

with DAG(
    dag_id="detect_file",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_customers_csv",
        filepath="data/incoming/customers.csv",
        poke_interval=5,
        timeout=300,
        mode="poke"
    )