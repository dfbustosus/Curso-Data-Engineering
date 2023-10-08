from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from pathlib import Path
import pandas as pd
import json
import os

dag_path = os.getcwd()     #path original.. home en Docker

# Argumentos del dag
default_args = {
    'owner': 'DAVIDBU',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear instancia del dag
dag = DAG(
    'currency_exchange_dag',
    default_args=default_args,
    description='DAG para ETL de exchange data',
    schedule_interval=timedelta(days=1),  # Schedule interval
)

def extract_data():
    # Obtener fecha actual
    current_date = datetime.now()

    # Calcular tres meses atras de la fecha actual
    three_months_ago = current_date - timedelta(days=3 * 30)  # Assuming each month has 30 days

    # Formatear la fecha en 'YYYY-MM-DD'
    three_months_ago_str = three_months_ago.strftime('%Y-%m-%d')

    # Establecer el endpoint URL con el filtro de tres meses atras
    api_endpoint = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange'
    params = {
        'fields': 'country_currency_desc,exchange_rate,record_date',
        'filter': f'record_date:gte:{three_months_ago_str}'
    }

    # Hacer el API request 
    response = requests.get(api_endpoint, params=params)
    data = response.json()
    
    file_path = "{}/tmp/data.json".format(dag_path)
    with open(file_path, 'w') as file:
        json.dump(data['data'], file)
    print('Tarea 1 Ok')

def transform_data():
    # Convertir la raw data en DataFrame
    raw_data = "{}/tmp/data.json".format(dag_path)
    with open(raw_data, 'r') as file:
        data = json.load(file)
    tabla = pd.DataFrame(data)
    tabla['exchange_rate'] = tabla['exchange_rate'].astype(float)
    # Extraer la letra inicial de cada pais como group key
    tabla['group_key'] = tabla['country_currency_desc'].str[0]
    # Agrupar y calcular la media 
    mean_exchange_rate_by_group = tabla.groupby('group_key')['exchange_rate'].mean().reset_index()
    # Escribir en la ruta de destino
    mean_exchange_rate_by_group.to_csv("{}/tmp/stats.csv".format(dag_path), index=False)

# Definir los operadores con PythonOperator
extract_task = PythonOperator(
    task_id='extraer_data',
    python_callable=extract_data,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id='transformar_data',
    python_callable=transform_data,
    dag=dag,
)


# Definir dependencias
extract_task >> transform_and_load_task
