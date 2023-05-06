from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
# Operadores
from airflow.operators.python_operator import PythonOperator
# funcion days_ago
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os
# Obtener el dag del directorio
dag_path = os.getcwd()

# funcion de transformacion de datos
def transformar_data(exec_date):
    try:
        print(f"Adquiriendo data para la  fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
        # Leer la data
        booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
        client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
        hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)
        # Hacer el merge de booking con client
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)
        # hacer merge de booking, client & hotel
        data = pd.merge(data, hotel, on='hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace=True)
        # convertir a formato fecha 
        data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)
        # Convertir todo a GBP currency con filtros
        data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
        data.currency.replace("EUR", "GBP", inplace=True)
        # remover columnas innecesarias
        data = data.drop('address', 1)
        # cargar la data procesada
        output_dir = Path(f'{dag_path}/processed_data/{file_date_path}')
        output_dir.mkdir(parents=True, exist_ok=True)
        data.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False, mode='a')
        #data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e

# funcion de carga de datos en base de datos
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
    conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    client_id INTEGER NOT NULL,
                    booking_date TEXT NOT NULL,
                    room_type TEXT(512) NOT NULL,
                    hotel_id INTEGER NOT NULL,
                    booking_cost NUMERIC,
                    currency TEXT,
                    age INTEGER,
                    client_name TEXT(512),
                    client_type TEXT(512),
                    hotel_name TEXT(512)
                );
             ''')
    #records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv") #leer tabla creada
    processed_file = f"{dag_path}/processed_data/{file_date_path}/{file_date_path.replace('/', '_')}.csv"
    records = pd.read_csv(processed_file)
    records.to_sql('booking_record', conn, index=False, if_exists='append')
    #records.to_sql('booking_record', conn, if_exists='replace', index=False) # mandarla a la base de datos


# argumentos por defecto para el DAG
default_args = {
    'owner': 'DavidBU',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Agrega records de reservas para analisis',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)


task_1 >> task_2
