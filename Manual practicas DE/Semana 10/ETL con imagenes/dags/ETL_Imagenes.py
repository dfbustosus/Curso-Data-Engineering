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

# 1. Definir el DAG
dag = DAG(
    dag_id="descargar_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

# 2. Operador Bash para descargar data de la API a la carpeta tmp en formato launches.json
download_launches = BashOperator(
    task_id="descargar_launches",
    #bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    bash_command="curl -o {}/tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'".format(dag_path),
    dag=dag,
)

# 3. Descargar imagenes dentro del json
def _get_pictures():
    # 3.1 Asegurarse que el directorio exista 
    pathlib.Path(dag_path+"/tmp/images").mkdir(parents=True, exist_ok=True)
    # 3.2 Descargar todas las imagenes en del launches.json
    with open(dag_path+"/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = dag_path+f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f: # escritura
                    f.write(response.content)
                    print(f"Descargada {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} parece ser una invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"No se pudo conectar a {image_url}.")

# 4. Operador asociado a la función anterior
get_pictures = PythonOperator(
    task_id="obtener_pictures",
    python_callable=_get_pictures,
    dag=dag,)

# 5. Notificación de imagenes listas
notify = BashOperator(
    task_id="notificar",
    #bash_command='echo "Hay actualmente $(ls /tmp/images/ | wc -l) imagenes."',
    bash_command='echo "Hay actualmente $(ls {}/tmp/images/ | wc -l) imagenes descargadas."'.format(dag_path),
    dag=dag,)

# Orden de tareas    
download_launches >> get_pictures >> notify
