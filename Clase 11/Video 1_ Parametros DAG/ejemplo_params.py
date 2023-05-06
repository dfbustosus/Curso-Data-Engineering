from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Aqui creamos los default arguments
default_args={
    'owner': 'DavidBU',
    'depends_on_past': True,
    'email': ['dafbustosus@unal.edu.co'],
    'email_on_retry':False,
    'email_on_failure': False,
    'retries':5,
    'retry_delay': timedelta(minutes=1)
}

# Creamos el objeto DAG
with DAG(
    dag_id="dag_para_explicar_atributos",
    default_args= default_args,
    description="En este DAG explicamos atributos importantes de los DAGs",
    start_date=datetime(2022,8,3,2),
    #end_date=datetime(2022,8,1,10),
    tags=['Ejemplo',"David" ,'Params'], # estos apareceran debajo del DAG en la plataforma
    schedule_interval="@daily") as dag:
    task1= BashOperator(task_id='primera_tarea',
    bash_command='echo Primer tarea mi nombre es David!'
    )

    task2 =BashOperator(
        task_id='segunda_tarea',
        bash_command="echo Segunda tarea completada soy Ingeniero"
    )

    task3 =BashOperator(
        task_id= 'tercera_tarea',
        bash_command='echo Tercera tarea completada me gusta programar'
    )

    task1 >> task2 >> task3