
from datetime import timedelta,datetime
from email import message
from pathlib import Path
from airflow.models import DAG, Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
##from datetime import datetime
from sqlalchemy import create_engine
import requests
import json
import pandas as pd
import psycopg2
import os
from psycopg2.extras import execute_values
import smtplib



default_args = {
    'owner': 'marcoss',
    'start_date': datetime(2023, 6, 10),
    'schedule_interval': '0 0 * * *'  
}

dag = DAG('etl_imdb', default_args=default_args)

def conectar_api():
    api_key = "k_a1r7e30i"  
    url = f'https://imdb-api.com/en/API/MostPopularMovies/{api_key}'
    print(url)

    response = requests.get(url)
    data = json.loads(response.text)
    df = pd.DataFrame(data["items"])
    csv_path = 'C:\data.csv'  
    df.to_csv(csv_path, index=False)
    return csv_path
    


def validar_dataframe(csv_path, **kwargs):
    df = pd.read_csv(csv_path)
    ti = kwargs['ti']
    # Verificar nulos
    if df.isnull().values.any():
        print("¡Se encontraron valores nulos en el DataFrame!")

    # Verificar duplicados
    if df.duplicated().any():
        print("¡Se encontraron registros duplicados en el DataFrame!")


def cargar_en_redshift(csv_path, **kwargs):
    df = pd.read_csv(csv_path)
    ti = kwargs['ti']
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    database = "data-engineer-database"
    user = "marcosmsanchez_coderhouse"
    password = "aH9v01R8ITJ0"

    try:
        conn = psycopg2.connect(
            host=url,
            dbname=database,
            user=user,
            password=password,
            port='5439'
        )
        print("Connected to Redshift successfully!")
        
        table_name = "etl_tabla1"  
        
        dtypes = df.dtypes
        cols = list(dtypes.index)
        tipos = list(dtypes.values)
        type_map = {'int64': 'INT', 'float32': 'FLOAT','float64': 'FLOAT', 'object': 'VARCHAR(500)'}
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        
        # Definir formato SQL VARIABLE TIPO_DATO
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        
        # Combine column definitions into the CREATE TABLE statement
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            );
            """
        
        # Crear la tabla
        cur = conn.cursor()
        cur.execute(table_schema)
        
        # Generar los valores a insertar
        values = [tuple(x) for x in df.to_numpy()]
        
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        
        # Ejecutar la transacción para insertar los datos
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        
        print('Proceso terminado')
        conn.close()
    
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)


def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('marcosmsanchez@gmail.com','pfsqruzxhutiplqm')
        subject='Tarea ETL'
        body_text='SE GENERO CORRECTAMENTE LA TAREA'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('marcosmsanchez@gmail.com','marcosmsanchez@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')



task_1 = PythonOperator(
    task_id='conectar_api',
    python_callable=conectar_api,
    dag=dag
)

task_2 = PythonOperator(
    task_id='validar_dataframe',
    python_callable=validar_dataframe,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids="conectar_api") }}'],
    dag=dag
)

task_3 = PythonOperator(
    task_id='cargar_en_redshift',
    python_callable=cargar_en_redshift,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids="conectar_api") }}'],
    dag=dag
)

task_4= PythonOperator(
        task_id='enviar_mail',
        python_callable=enviar,
        dag=dag
    )


task_1>>task_2 >>task_3 >>task_4