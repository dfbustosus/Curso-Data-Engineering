from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner':'DavidBU',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

with DAG(
    default_args=default_args,
    dag_id='dag_con_backfilling',
    description= 'Dag con catchup',
    start_date=datetime(2022,9,1),
    schedule_interval='@daily',
    catchup=False
    ) as dag:
    task1=BashOperator(
        task_id='tarea1',
        bash_command='echo Esto es un DAG con catchup'
    )
    task1