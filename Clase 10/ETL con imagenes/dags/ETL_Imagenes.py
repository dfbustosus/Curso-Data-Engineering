# Invoke-WebRequest -Uri "https://ll.thespacedevs.com/2.0.0/launch/upcoming" 
import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

dag_path = os.getcwd()     #path original.. home en Docker

dag = DAG(
    dag_id="descargar_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id="descargar_launches",
    #bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    bash_command="curl -o {}/tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'".format(dag_path),
    dag=dag,
)

def _get_pictures():
    # Asegurarse que el directorio exista 
    pathlib.Path(dag_path+"/tmp/images").mkdir(parents=True, exist_ok=True)
    # Descargar todas las imagenes en del launches.json
    with open(dag_path+"/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = dag_path+f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                    print(f"Descargada {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} parece ser una invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"No se pudo conectar a {image_url}.")

get_pictures = PythonOperator(
    task_id="obtener_pictures",
    python_callable=_get_pictures,
    dag=dag,)

notify = BashOperator(
    task_id="notificar",
    #bash_command='echo "Hay actualmente $(ls /tmp/images/ | wc -l) imagenes."',
    bash_command='echo "There are currently $(ls {}/tmp/images/ | wc -l) images."'.format(dag_path),
    dag=dag,)

# Orden de tareas    
download_launches >> get_pictures >> notify
