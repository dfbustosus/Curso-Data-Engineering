import requests
import pandas as pd
import json
from datetime import date, timedelta
import psycopg2
import time
import re
from dotenv import load_dotenv
import os

url="https://archive-api.open-meteo.com/v1/archive?latitude={0}&longitude={1}\
&start_date={2}&end_date={3}&models=best_match&timezone=America%2FNew_York\
&daily=\
temperature_2m_max,\
temperature_2m_min,\
sunrise,\
sunset,\
precipitation_sum,\
windspeed_10m_max"

# Coordenadas de Manhattan.
latitude = 40.776676
longitude = -73.971321

load_dotenv() # Prepara las credenciales .env.

columnas = ["daily_time", 
            "daily_temperature_2m_max",
            "daily_temperature_2m_min",
            "daily_sunrise",
            "daily_sunset",
            "daily_precipitation_sum",
            "daily_windspeed_10m_max"]

# Dictionary to transform all dtypes to usable types
dic_types = {
    "latitude": 'float',
    "longitude": 'float',
    "generationtime_ms": 'float',
    "utc_offset_seconds": 'float',
    "timezone": 'string',
    "timezone_abbreviation": 'string',
    "elevation": 'float',
    "daily_units_time" : 'string',
    "daily_units_temperature_2m_max": 'string',
    "daily_units_temperature_2m_min": 'string',
    "daily_units_sunrise": 'string',
    "daily_units_sunset": 'string',
    "daily_precipitation_sum": 'string',
    "daily_windspeed_10m_max": 'string',
    "daily_time": 'string',
    "daily_temperature_2m_max": 'float',
    "daily_temperature_2m_min": 'float',
    "daily_sunrise": 'string',
    "daily_sunset": 'string',
    "daily_precipitation_sum": 'float',
    "daily_windspeed_10m_max": 'float'
}

# Dictionary with dtypes from pandas and sql data types.
dic_pdtosql = {
    'object': 'VARCHAR',
    'int64': 'INTEGER',
    'float64': 'FLOAT',
    'bool': 'BIT',
    'datetime64': 'DATETIME',
    'timedelta64': 'INTERVAL',
    'category': 'VARCHAR',
    'string' : 'VARCHAR'
}

# Conection to the database.
def conection():
    conn = psycopg2.connect(
        host=os.getenv('REDSHIFT_HOST'),
        database=os.getenv('REDSHIFT_DATABASE'),
        user= os.getenv('REDSHIFT_USERNAME'),
        password= os.getenv('REDSHIFT_PASSWORD'),
        port=os.getenv('REDSHIFT_PORT'))
    return conn

# Table creation.
def create_table(table_name): # No funciona el DROP porque no se hace el commit
    cursor.execute("DROP TABLE IF EXISTS daily_weather")
    print("Table droped.")
    columns = []
    df = apic(start_date, end_date)
    for column in df.columns:
        data_type = dic_pdtosql[str(df[column].dtype)]
        columns_sql = '{} {}'.format(column, data_type)
        columns.append(columns_sql)

    table_query = "CREATE TABLE {} ({})".format(table_name, ", ".join(columns))
    #print(table_query)
    #cursor.execute(table_query)

# Muestra DF en forma de tabla
def mostrar_fila_como_tabla(dataframe, fila=0):
    columnas = dataframe.columns.tolist()
    valores = dataframe.iloc[fila].values.tolist()
    dtypes = dataframe.dtypes.tolist()
    df_fila = pd.DataFrame({'Columna': columnas, 'Valor': valores, 'Tipo de Dato': dtypes})
    print(df_fila.to_string(index=False))

# Llama la API y normaliza los datos para el df.
def apic(start_date, end_date):
        end_date = start_date
        full_url = url.format(latitude,longitude,start_date,end_date)
        #print(full_url)
        response = requests.get(full_url) 
        print("Server response: \033[2;32;40m{}\033[0;37;40m.".format(response.status_code)) # Referencia https://www.kaggle.com/general/273188
        json_data = json.loads(response.text, parse_constant=lambda x: x.replace("'", '"'))
        df = pd.json_normalize(json_data, sep="_")
        #df = df.rename(columns=lambda x: x.replace(".", "_"))
        #print(df.columns)
        #df.iloc[0] = df.iloc[0].apply(lambda x: x.replace("[", ""))
        #df.replace(to_replace=['\[', '\]'], value='', regex=True, inplace=True)
        #df = df.replace('\[|\]', '', regex=True)
        for _ in columnas:
            df[_] = df[_].astype(str).apply(lambda x: re.sub(r'\[|\]', '', x))
        #mostrar_fila_como_tabla(df, 0)
        df = df.astype(dic_types)
        #print("Dataframe normalized.")
        return df
#
def insert_data(start_date, end_date, table_name):
    fin = end_date
    while fin >= start_date: # Finaliza bucle cuando llega a la fecha seteada
        print("------------------------------------------------------------------.")
        print("Working.")
        df = apic(start_date, end_date,)
        print("Obtained data from the day {}, last day will be {}".format(start_date, end_date))
        add_row(table_name, df)
        print("Row added.")
        start_date = start_date + timedelta(1) # Suma un dia
        time.sleep(1)
#
def add_row(table_name, df):
    row = df.iloc[0]
    column = ', '.join(row.index)
    values = ', '.join(['%({0})s'.format(col) for col in row.index])
    query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column, values)
    cursor.execute(query, row)
    conn.commit()

start_date = date(2022,1,1) # 2023-04-04
end_date = date(2022,12,31) # Fecha FIN
table_name = "daily_weather_2022"

# Try/Expect.
conn = conection()
cursor = conn.cursor()

with cursor:
    #print("Creating \"{}\" table.".format(table_name))
    #create_table(table_name)
    #print("Table \"{}\" successfully created.".format(table_name))
    print("Initializing data insertion.")
    insert_data(start_date, end_date, table_name)

conn.commit()
conn.close()
