from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'DavidBU',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

def aloha_david():
    print("Aloha Mundo soy yo!")

with DAG(
    default_args=default_args,
    dag_id='mi_primer_dar_con_PythonOperator',
    description= 'Nuestro primer dag usando python Operator',
    start_date=datetime(2022,8,1,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='aloha_david',
        python_callable= aloha_david,
    )

    task1
