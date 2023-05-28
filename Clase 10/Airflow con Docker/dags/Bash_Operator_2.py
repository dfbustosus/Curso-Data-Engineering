from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'DavidBU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='dag_Bash_Python_Operators',
    default_args=default_args,
    description='Este es el primer DAG que creamos',
    start_date=datetime(2023, 5, 28, 2),
    schedule_interval='@daily'
)

def print_hola():
    print('Hola mundo soy yo David!')

def print_adios():
    print('Hasta luego a todos!')

def echo_task():
    return BashOperator(
        task_id='echo_task',
        bash_command='echo "Hola desde my bash!"',
        dag=dag
    )

def crear_archivo_task():
    return BashOperator(
        task_id='crear_archivo_task',
        bash_command='touch /tmp/test.txt',
        dag=dag
    )

def mover_archivo_task():
    return BashOperator(
        task_id='mover_archivo_task',
        bash_command='mv /tmp/test.txt /tmp/test2.txt',
        dag=dag
    )

def limpiar_task():
    return BashOperator(
        task_id='limpiar_task',
        bash_command='rm /tmp/test2.txt',
        dag=dag
    )

hello_task = PythonOperator(
    task_id='hola_task',
    python_callable=print_hola,
    dag=dag
)

adios_task = PythonOperator(
    task_id='adios_task',
    python_callable=print_adios,
    dag=dag
)

echo_op = echo_task()
crear_archivo_op = crear_archivo_task()
mover_archivo_op = mover_archivo_task()
cleanup_op = limpiar_task()

hello_task >> echo_op
hello_task >> crear_archivo_op
crear_archivo_op >> mover_archivo_op
[echo_op, mover_archivo_op] >> cleanup_op
adios_task >> cleanup_op