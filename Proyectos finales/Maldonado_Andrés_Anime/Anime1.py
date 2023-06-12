from datetime import datetime, timedelta
from airflow import DAG
from email import message
import smtplib
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json
from pprint import pprint
import re
import pandas as pd
from psycopg2.extras import execute_values
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

##Se define los argumento del DAG
default_args = {
    'owner': 'Maldonado_Andres',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    ##'start_date': datetime(2023, 5, 17, 22, 0)  # Fecha y hora de inicio del DAG
}
#Se utiliza expresiones regualres para modificar el formato de la duración
def convertir_formato(tiempo):
    patron_horas = r'(\d+)\s*hr\s*(\d+)\s*min'
    patron_minutos = r'(\d+)\s*min\s*per\s*ep'
    match_horas = re.match(patron_horas, tiempo)
    if match_horas:
        horas = match_horas.group(1)
        minutos = match_horas.group(2)
        return f"{horas}hs - {minutos}min"

    match_minutos = re.match(patron_minutos, tiempo)
    if match_minutos:
        minutos = match_minutos.group(1)
        return f"{minutos}min"

    return tiempo
## Se obtienen los datos de la API
def obtener_los_datos():
    url = 'https://api.jikan.moe/v4/top/anime'
    response = requests.get(url)
    if response.status_code == 200:
        response_json = json.loads(response.text)
        lista = []
        for i in response_json["data"]:
            id = i["mal_id"]
            url = i["url"]
            titulo = i["title"]
            tipo = i["source"]
            cant_episodios = i["episodes"]
            duracion = convertir_formato(i["duration"])
            ranking = i["rank"]
            popularidad = i["popularity"]
            seguidores = i["members"]
            dic = {
                "id": id,
                "url": url,
                "titulo": titulo,
                "tipo": tipo,
                "cant_episodios": cant_episodios,
                "duracion": duracion,
                "ranking": ranking,
                "popularidad": popularidad,
                "seguidores": seguidores
            }
            lista.append(dic)
        return lista
    else:
        return []

## Se graban los datos de la api en la BD
def grabo_los_datos(**context):
    lista = context['ti'].xcom_pull(task_ids='obtener_los_datos')
    if lista:
        df = pd.DataFrame(lista)
        df = df.drop_duplicates()
        table_name = 'EntregaFinal4'
        columns = ['id', 'url', 'titulo', 'tipo', 'cant_episodios', 'duracion', 'ranking', 'popularidad', 'seguidores']
        values = [tuple(x) for x in df.to_numpy()]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        hook = PostgresHook(postgres_conn_id='Entrega_final_Redshift')
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                execute_values(cur, insert_sql, values)
                cur.execute("COMMIT")
#Se parametriza el email
def enviar(tipo):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('CursoMaldonadoData2023@gmail.com','rlwtgsyvuthrzhkh')
        
        if tipo == 'inicio':
            subject = 'Se ha iniciado el proceso'
            body_text = 'El proceso ha comenzado de manera correcta.'
        elif tipo == 'final':
            subject = 'La tarea finalizó exitosamente'
            body_text = 'La tarea finalizó correctamente.'
        
        #subject="Aviso Tarea"
        #body_text="La tarea finalizó correctamente!!!!"
        message='Subject: {}\n\n{}'.format(subject,body_text)
        message=message.encode('utf-8')
        x.sendmail('CursoMaldonadoData2023@gmail.com','lutonhome2005@gmail.com',message)
        print('Tarea fnalizada exitosamente')
    except Exception as exception:
        print(exception)
        print('Failure')



def enviar_aviso():
    print("Los procesos han finalizado con éxito")


## Se establecen las tareas
with DAG(
    default_args=default_args,
    dag_id='Entrega_Final_Maldonado',
    description='Mi primer dag usando Python Operator',
    start_date=datetime(2023, 6, 1),
    schedule_interval='0 0 * * *'  # Esto significa que se ejecuta todos los días a la medianoche
) as dag:
    task1 = PostgresOperator(
    task_id='crear_tabla_Anime',
    postgres_conn_id='Entrega_final_Redshift',
    sql="""
        CREATE TABLE IF NOT EXISTS EntregaFinal4 (
            id bigint,
            url varchar(100),
            titulo varchar(300),
            tipo varchar(50),
            cant_episodios int,
            duracion varchar(50),
            ranking int,
            popularidad int,
            seguidores bigint
        )
    """
)

    task2 = PythonOperator(
        task_id='obtener_los_datos',
        python_callable=obtener_los_datos,
)

    task3 = PythonOperator(
        task_id='grabo_los_datos',
        python_callable=grabo_los_datos,
)
    #task4 = PythonOperator(
    #task_id='enviar_aviso_log',
    #python_callable=enviar_aviso,
    #dag=dag
    
    task4=PythonOperator(
        task_id='dag_envio_email_fin',
        python_callable=enviar,
        op_kwargs={'tipo': 'final'}
)
    tarea_5=PythonOperator(
        task_id='dag_envio_email_inicio',
        python_callable=enviar,
        op_kwargs={'tipo': 'inicio'}
    )


    tarea_5 >> task1
    task1 >> task2 >> task3 >> task4