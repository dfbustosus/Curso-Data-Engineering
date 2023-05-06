from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'DavidBU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}

with DAG(
    dag_id="mi_primer_dag_v3",
    default_args= default_args,
    description="Este es el primer DAG que creamos",
    start_date=datetime(2022,8,1,2),# esto dice que debemos iniciar el 1-Ago-2022 y a un intervalo diario
    schedule_interval='@daily' ) as dag:
    task1= BashOperator(task_id='primer_task',
    bash_command='echo hola mundo, esta es nuestra primera tarea!'
    )

    task2 =BashOperator(
        task_id='segunda_tarea',
        bash_command="echo hola, soy la tarea 2 y sere corrida luego de la Tarea 1"
    )

    task3 =BashOperator(
        task_id= 'tercera_tarea',
        bash_command='echo hola, soy la tarea 3 y sere corrida luego de Tarea 1 al mismo tiempo que Tarea 2'
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)