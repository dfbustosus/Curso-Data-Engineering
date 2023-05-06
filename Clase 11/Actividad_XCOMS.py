from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from random import randint 

def _performance():
    return randint(1,10)

def _elegir_mejor_valor(ti):  
    #esta funcion trea los tres valores de las tareas creadas    
    performance=ti.xcom_pull(task_ids=['empleado_A','empleado_B','empleado_C'])
    mejor_performance=max(performance) # elige el valor mas grande de la lista
    # condicional para elegir el valor mas grande
    if mejor_performance == performance[0]:
        [ti.xcom_push(key='mejor_algo',value='empleado_A'),ti.xcom_push(key='mejor_performance',value=mejor_performance)]
    elif mejor_performance == performance[1]:
        [ti.xcom_push(key='mejor_algo',value='empleado_B'),ti.xcom_push(key='mejor_performance',value=mejor_performance)]
    else:
        [ti.xcom_push(key='mejor_algo',value='empleado_C'),ti.xcom_push(key='mejor_performance',value=mejor_performance)]
    return 'usar_algo' # devuelve una tarea en este caso

def _usar_tarea(ti):
    nombre= ti.xcom_pull(key='mejor_algo',task_ids='elegir_mejor_valor')
    performance_tarea= ti.xcom_pull(key='mejor_performance', task_ids='elegir_mejor_valor')
    print('Usando', nombre + ' con valor de:', performance_tarea)


default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2022,9,7),
    'end_date': datetime(2022,12,20),
    'depends_on_past': False,
    'email': ['davidbu@gcp.com'],
    'email_on_failure': False
}
with DAG(
    dag_id ='dag_xcoms_facil',
    default_args=default_args,
    schedule_interval='@daily',
    catchup =False ) as dag:
    empleado_A= PythonOperator(
        task_id= 'empleado_A',
        python_callable= _performance
    )
    empleado_B= PythonOperator(
        task_id= 'empleado_B',
        python_callable= _performance
    )
    empleado_C= PythonOperator(
        task_id= 'empleado_C',
        python_callable= _performance
    )
    elegir_mejor =BranchPythonOperator(
        task_id= 'elegir_mejor_valor',
        python_callable=_elegir_mejor_valor
    )
    usar_tarea= PythonOperator(
        task_id='usar_algo',
        python_callable=_usar_tarea
    )

    [empleado_A,empleado_B,empleado_C] >> elegir_mejor >> usar_tarea
