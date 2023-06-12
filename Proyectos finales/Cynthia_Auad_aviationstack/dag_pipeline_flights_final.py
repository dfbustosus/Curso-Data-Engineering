# Libraries
from datetime import timedelta,datetime
from pathlib import Path
import requests
import json
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
import pandas as pd
import os
import smtplib

# Operadores
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator


# Obtener el dag del directorio
dag_path = os.getcwd()     #esto direcciona al path original.. el home de la maquina de docker

key_red = os.environ["PASSWORD_REDSHIFT"]
key_fly = os.environ["PASSWORD_flight"]
key_mail = os.environ["PASSWORD_gmail"]

redshift_conn = {
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'username': 'auad03_coderhouse',
    'database': 'data-engineer-database',
    'port': '5439'
}

# funcion de extraccion de datos
def extraer_data(exec_date):
    try:  #muestro fecha de ejecucion
        print(f"Adquiriendo data para la  fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
        #--- Extraer la data  --------
        url = "http://api.aviationstack.com/v1/flights?access_key="
        params = key_fly
        response = requests.get(url+params)
        if response:
                print('Success!')
        else:
                print('An error has occurred.') 
        api_response = response.json()
        df_flights = pd.json_normalize(api_response, record_path=['data'])
        output_dir = Path(f'{dag_path}/raw_data/{file_date_path}')  ##busco la carpeta raw_data y almaceno
        output_dir.mkdir(parents=True, exist_ok=True)
        df_flights.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False, mode='a')
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# funcion de transformacion de datos
def transformar_data(exec_date, ti):       
        print(f"Transformando la data para la fecha: {exec_date}") 
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
        raw_file = f"{dag_path}/raw_data/{file_date_path}/{file_date_path.replace('/', '_')}.csv"
        df_flights = pd.read_csv(raw_file)
        # ------- Transformacion de la data ----------------
        # Eliminar variables con muchos nulos
        df = df_flights.drop(['aircraft', 'live', 'departure.gate', 'departure.delay', 'departure.actual', 'departure.estimated_runway', 'departure.actual_runway', 
                       'arrival.gate', 'arrival.baggage', 'arrival.delay', 'arrival.actual', 'arrival.estimated_runway', 'arrival.actual_runway', 
                       'flight.codeshared', 'flight.codeshared.airline_name', 'flight.codeshared.airline_iata', 'flight.codeshared.airline_icao', 'flight.codeshared.flight_number', 
                       'flight.codeshared.flight_iata', 'flight.codeshared.flight_icao'], axis=1)
        # Renombrar columnas
        df=df.rename(columns={'departure.airport':'dep_airport','departure.timezone':'dep_timezone','departure.iata':'dep_iata','departure.icao':'dep_icao','departure.terminal':'dep_terminal',\
                          'departure.scheduled':'dep_scheduled','departure.estimated':'dep_estimated', 'arrival.airport':'arr_airport', 'arrival.timezone':'arr_timezone',\
                          'arrival.iata':'arr_iata', 'arrival.icao':'arr_icao','arrival.terminal':'arr_terminal', 'arrival.scheduled':'arr_scheduled', 'arrival.estimated':'arr_estimated',\
                          'airline.name':'airline_name', 'airline.iata':'airline_iata','airline.icao':'airline_icao', 'flight.number':'flight_number', 'flight.iata':'flight_iata',\
                          'flight.iata':'flight_iata', 'flight.icao':'flight_icao'})
        # si flight_date es null lo elimino
        df.drop(df[df.flight_date.isnull()].index, inplace = True)
        numero = int(df['flight_date'].count())
        print('Cantidad de vuelos descargados:', numero)
        ti.xcom_push(key='transform_data', value= numero)
        # convertir a formato fecha 
        df["flight_date"] = pd.to_datetime(df["flight_date"], infer_datetime_format=True)
        df["dep_scheduled"] = pd.to_datetime(df["dep_scheduled"], infer_datetime_format=True)
        df["dep_estimated"] = pd.to_datetime(df["dep_estimated"], infer_datetime_format=True)
        df["arr_scheduled"] = pd.to_datetime(df["arr_scheduled"], infer_datetime_format=True)
        df["arr_estimated"] = pd.to_datetime(df["arr_estimated"], infer_datetime_format=True)
        # Calcular la duracion agendada del vuelo en minutos
        schedule = df.arr_scheduled - df.dep_scheduled
        df['duration_scheduled_min'] = schedule / timedelta(minutes=1)
        # Calcular la duracion estimada del vuelo en minutos
        estimated = df.arr_estimated - df.dep_estimated
        df['duration_estimated_min'] = estimated / timedelta(minutes=1)
        # cargar la data procesada
        output_dir = Path(f'{dag_path}/processed_data/{file_date_path}')
        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False, mode='a')


# funcion de carga de datos en base de datos
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
    PDW = key_red
    # Crear la conexion
    engine = create_engine(f'redshift+psycopg2://{redshift_conn["username"]}:{PDW}@{redshift_conn["host"]}:{redshift_conn["port"]}/{redshift_conn["database"]}')
    engine.execute('''
                CREATE TABLE IF NOT EXISTS realtime_flights_airflow_new (
                    flight_date DATE NOT NULL,
                    flight_status VARCHAR(20),
                    dep_airport VARCHAR(70),
                    dep_timezone VARCHAR(70),
                    dep_iata CHAR(10), 
                    dep_icao CHAR(10),
                    dep_terminal CHAR(10),
                    dep_scheduled datetime,
                    dep_estimated datetime,
                    arr_airport VARCHAR(70),    
                    arr_timezone VARCHAR(70),    
                    arr_iata CHAR(10), 
                    arr_icao CHAR(10),        
                    arr_terminal CHAR(10),  
                    arr_scheduled datetime, 
                    arr_estimated datetime,
                    airline_name VARCHAR(70),       
                    airline_iata CHAR(10), 
                    airline_icao CHAR(10),
                    flight_number int,
                    flight_iata CHAR(10),
                    flight_icao CHAR(10),
                    duration_scheduled_min float,
                    duration_estimated_min float
                    );
             ''')
    #leer tabla creada
    processed_file = f"{dag_path}/processed_data/{file_date_path}/{file_date_path.replace('/', '_')}.csv"
    records = pd.read_csv(processed_file)
    # mandarla a la base de datos
    records.to_sql('realtime_flights_airflow_new', engine, index=False, if_exists='append')

# Envio de mensaje
def enviar(ti):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('auad03@gmail.com', key_mail) 
        numero = int(ti.xcom_pull(key='transform_data', task_ids='transform_data'))    
        subject='Nueva data vuelos en tiempo real'
        body_text='Se agregaron nuevos vuelos en tiempo real: '
        num_ = '{}'.format(numero)
        body_text_new = body_text + num_
        print(body_text_new)
        message='Subject: {}\n\n{}'.format(subject, body_text_new)
        x.sendmail('auad03@gmail.com','cauad85@gmail.com',message)
        print('Exito')   
    except Exception as exception:
        print(exception)
        print('Failure')

# argumentos por defecto para el DAG
default_args = {
    'owner': 'CynthiaA',
    'start_date': datetime(2023,6,5),
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}

flight_dag = DAG(
    dag_id='realtime_data',
    default_args=default_args,
    description='Agrega data de vuelvos en tiempo real',
    schedule_interval="@daily",
    catchup=False
)

# TAREAS
task_1 = PythonOperator(
    task_id='extract_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=flight_dag,
)

task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=flight_dag,
)

task_3 = PythonOperator(
    task_id='load_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=flight_dag,
)

task_4 = PythonOperator(
    task_id='send_mail',
    python_callable=enviar,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=flight_dag,
)

# Orden de ejecucion de las tareas
task_1 >> task_2 >> task_3 >> task_4