import requests
import pandas as pd
import json
from datetime import date, timedelta, datetime
import psycopg2
import time
import re
from dotenv import load_dotenv
import os
from airflow import DAG
from pathlib import Path
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import proyecto_coderhouse as pc

dag_path = os.getcwd()

load_dotenv()

default_args = {
    'owner': 'Tmuser',
    'start_date': datetime(2023,6,5),
    'retries':5,
    'retry_delay': timedelta(minutes=1)
}

primer_dag = DAG(
    dag_id='Yesterday_Weather',
    default_args=default_args,
    description='Consulta el tiempo de ayer.',
    schedule_interval="@daily",
    catchup=True
)

task_1 = PythonOperator(
    task_id='insert_data',
    python_callable=pc.insert_data,
    op_args=["{{ ds }} {{ execution_date }}"],
    dag=primer_dag,
)

task_1 #>> task_2