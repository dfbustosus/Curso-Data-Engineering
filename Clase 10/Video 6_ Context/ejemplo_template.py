import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag= DAG (
    dag_id= "dag_print_context",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
    tags=['Ejemplo',"David"]# estos apareceran debajo del DAG en la plataforma
)

def _print_context(**context):
    print(context) # el context es un diccionario con todas las variables por default
    start=context["execution_date"],
    end=context['next_execution_date'],
    print(f"Inicio:{start}, Fin: {end}")

print_context= PythonOperator(
    task_id="print_context",
    python_callable=_print_context, #nombre de la funcion
    dag=dag
)