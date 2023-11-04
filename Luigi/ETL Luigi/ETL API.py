# pip install psycopg2, luigi, requests, python-dotenv, pandas
import luigi
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import psycopg2
import urllib.parse
load_dotenv()  # Cargar environment

# Extraer Data de API API
class APICallTask(luigi.Task):
    start_date = luigi.Parameter(default="2020-01-01")
    def run(self):
        API_BASE_URL = "https://apis.datos.gob.ar/series/api/"
        ids = ["75.3_IEC_0_M_26"]
        params = {"ids": ",".join(ids), "start_date": self.start_date}
        api_call = f"{API_BASE_URL}series?{urllib.parse.urlencode(params)}"
        result = requests.get(api_call).json()
        df = pd.DataFrame(result['data'], columns=["date_from", "millones_dolares"])
        df.to_csv('api_result.csv', index=False)  # Guardarlo como CSV
    def output(self):
        return luigi.LocalTarget("api_result.csv")  # Salida

# Transform and Load
class DatabaseLoadTask(luigi.Task):
    # Invocar requerimiento tarea anterior
    def requires(self):
        return APICallTask()
    def run(self):
        # conectarse a la BD
        conn = psycopg2.connect(
            host=os.getenv('AWS_REDSHIFT_HOST'),
            port=os.getenv('AWS_REDSHIFT_PORT'),
            dbname=os.getenv('AWS_REDSHIFT_DBNAME'),
            user=os.getenv('AWS_REDSHIFT_USER'),
            password=os.getenv('AWS_REDSHIFT_PASSWORD')
        )
        # Apuntar a la BD
        cursor = conn.cursor()
        # Leer el archivo en memoria
        with self.input().open('r') as f:
            df = pd.read_csv(f)
            # Crear tabla si no existe
            cursor.execute("CREATE TABLE IF NOT EXISTS exportaciones_cereales (date_from DATE, millones_dolares DECIMAL);")
            # Inserts por fila
            for index, row in df.iterrows():
                cursor.execute("INSERT INTO exportaciones_cereales (date_from, millones_dolares) VALUES (%s, %s);", (row['date_from'], row['millones_dolares']))
            # Guardar cambios en tabla
            conn.commit()
        # Cerrar puntero
        cursor.close()
    # Salida
    def output(self):
        return luigi.LocalTarget("load_complete.txt")

if __name__ == '__main__':
    luigi.build([DatabaseLoadTask()], local_scheduler=True)
