from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def download_data():
    # La API devuelve un usuario por cada consulta, por lo que ejecutamos un ciclo para obtener los usuarios deseados
    total_users = 20
    counter = 0
    data = []

    while counter < total_users:
        # Hacemos el request a la API
        response = requests.get('https://randomuser.me/api/')
        # Convertimos la data
        data_json = json.loads(response.text)
        data.append(data_json['results'][0])
        counter += 1
    
    # Convertimos la data a un DataFrame
    df = pd.json_normalize(data, sep='_')
    
    # Cargar la data en Redshift
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base="data-engineer-database"
    user="jese_salazar_coderhouse"
    pwd = '4eMu5U9tjv0J'

    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Connected to Redshift successfully!")
        
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

    # Función para cargar los datos


    def cargar_en_redshift(conn, table_name, dataframe):
        dtypes= dataframe.dtypes
        cols= list(dtypes.index )
        tipos= list(dtypes.values)
        type_map = {'int64': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)'}
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
        values = [tuple(x) for x in dataframe.to_numpy()]
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado')

    # Carga de datos en RedShift
    cargar_en_redshift(conn=conn, table_name='entregable_1', dataframe=df)

    conn.close()


## TAREAS

default_args={
    'owner': 'JeseSalazar',
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}

api_dag = DAG(
        dag_id="desafio3_pipeline",
        default_args= default_args,
        description="DAG para consumir API y vaciar datos en Redshift",
        start_date=datetime(2023,5,11,2),
        schedule_interval='@daily' 
    )

task1 = BashOperator(task_id='primera_tarea',
    bash_command='echo Iniciando...'
)

task2 = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=api_dag,
)

task3 = BashOperator(
    task_id= 'tercera_tarea',
    bash_command='echo Proceso completado...'
)
task1 >> task2 >> task3