from datetime import datetime, timedelta
from airflow import DAG 
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

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


def print_message():
    print("Llegó el archivo!")


file_sensor = FileSensor(
    task_id="sensar_archivo",
    poke_interval=60,
    timeout=60 * 30,
    filepath='/opt/airflow/data/compania1/data-*.csv',
)


imprimir = PythonOperator(
    task_id="print_message",
    dag=dag,
    python_callable=print_message
)


file_sensor >> imprimir
