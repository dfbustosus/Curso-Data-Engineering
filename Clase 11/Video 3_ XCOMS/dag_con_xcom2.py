import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime
import requests
import json
from datetime import timedelta

default_args = {
    'owner': 'David BU',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def conexion_api(ti):
    url =  'https://fakestoreapi.com'
    res = requests.get(f"{url}/products")
    data=json.loads(res.content)
    promedio=sum([x['price'] for x in data])/len(data)
    ti.xcom_push(key='descargando_data',value=promedio)

def analizando_data(ti):
    promedio_venta= ti.xcom_pull(key='descargando_data',
    task_ids='conexion_api')
    print('El promedio de precios es:', promedio_venta)

with DAG(
    dag_id='xcom_dag2',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 20),
    catchup=False
) as dag:
    obtener_data = PythonOperator(
        task_id='descargando_data',
        python_callable=conexion_api,
        #do_xcom_push=True
    )

    hacer_analisis = PythonOperator(
        task_id='obtener_promedio',
        python_callable=analizando_data,
        #do_xcom_push=True
    )

    obtener_data >> hacer_analisis
