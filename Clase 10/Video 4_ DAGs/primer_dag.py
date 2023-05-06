from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'DavidBU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}

with DAG(
    dag_id="mi_primer_dag",
    default_args= default_args,
    description="Este es el primer DAG que creamos",
    start_date=datetime(2022,8,1,2),# esto dice que debemos iniciar el 1-Ago-2022 y a un intervalo diario
    schedule_interval='@daily' ) as dag:
    task1= BashOperator(task_id='primer_task',
    bash_command='echo hola mundo, esta es nuestra primera tarea!'
    )

    task1