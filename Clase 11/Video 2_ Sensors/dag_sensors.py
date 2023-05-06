from datetime import datetime, timedelta
from airflow import DAG 
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.python import PythonSensor
from pathlib import Path

default_args={
    'owner': 'DavidBU',
    'depends_on_past': False,
    'email': ['dafbustosus@unal.edu.co'],
    'email_on_retry':False,
    'email_on_failure': False,
    'retries':10,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='data_sensors_DBU',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@daily',
    default_args=default_args
)

def _wait_for_supermarket(compania_id):
    compania_path = Path("/data/" + compania_id)
    data_files = compania_path.glob("data-*.csv")
    success_file = compania_path / "_SUCCESS"
    return data_files and success_file.exists()

wait_for_supermarket_1 = PythonSensor(
    task_id="esperando_por_compania_1",
    python_callable=_wait_for_supermarket,
    op_kwargs={"compania_id": "compania1"},
    dag=dag,
)