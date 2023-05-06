from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 8, 4)
}

def _entrenamiento_modelo(ti): # ticorresponde al objeto task instance 
    accuracy = uniform(0.01, 1.0)
    print(f'Accuracy de modelo\'s: {accuracy}')
    ti.xcom_push(key='accuracy_modelo', value=accuracy)

def _elegir_mejor_modelo(ti):
    print('elegir el mejor modelo')
    accuracies = ti.xcom_pull(key='accuracy_modelo', task_ids=['entrenando_modelo_A', 'entrenando_modelo_B', 'entrenando_modelo_C'])
    print(accuracies)

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    # primera tarea: descargar data... simulacion
    descargando_data = BashOperator(
        task_id='descargando_data',
        bash_command='sleep 3', #esperar por 3 segundos
        do_xcom_push=False
    )
    # segunda, tercera y cuarta tarea: entrenar modelos..... simulacion
    tarea_entrenamiento = [
        PythonOperator(
            task_id=f'entrenando_modelo_{task}',
            python_callable=_entrenamiento_modelo
        ) for task in ['A', 'B', 'C']]

    elegir_modelo = PythonOperator(
        task_id='elegir_modelo',
        python_callable=_elegir_mejor_modelo
    )

    descargando_data >> tarea_entrenamiento >> elegir_modelo
