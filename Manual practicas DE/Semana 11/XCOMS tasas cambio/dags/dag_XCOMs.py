from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests

default_args = {
    'start_date': datetime(2021, 1, 1)
}

def calcular_valor(**context):
    # Obtener la data de la API
    requestURL = 'https://api.exchangerate.host/latest'
    response = requests.get(requestURL).json()
    lista=['PEN','COP','UYU','BRL','ARS','CLP','BOB','VES','PYG','USD']
    rates=[round(response['rates'][x],4) for x in lista]
    currency_dict = dict(zip(lista, rates))
    
    # Guardar el resultado en una variable xcomStore the result in an XCOM variable
    context['ti'].xcom_push(key='result_PEN', value=currency_dict['PEN'])
    context['ti'].xcom_push(key='result_COP', value=currency_dict['COP'])
    context['ti'].xcom_push(key='result_UYU', value=currency_dict['UYU'])
    context['ti'].xcom_push(key='result_BRL', value=currency_dict['BRL'])
    context['ti'].xcom_push(key='result_ARS', value=currency_dict['ARS'])
    context['ti'].xcom_push(key='result_CLP', value=currency_dict['CLP'])
    context['ti'].xcom_push(key='result_BOB', value=currency_dict['BOB'])
    context['ti'].xcom_push(key='result_VES', value=currency_dict['VES'])
    context['ti'].xcom_push(key='result_PYG', value=currency_dict['PYG'])
    context['ti'].xcom_push(key='result_USD', value=currency_dict['USD'])
    print(f"Peru (Peruvian Sol): {currency_dict['PEN']}")
    print(f"Colombia (Colombian Peso): {currency_dict['COP']}")
    print(f"Uruguay (Uruguayan Peso): {currency_dict['UYU']}")
    print(f"Brazil (Brazilian Real): {currency_dict['BRL']}")
    print(f"Argentina (Argentine Real): {currency_dict['ARS']}")
    print(f"Chile (Chilenean Peso): {currency_dict['CLP']}")
    print(f"Bolivia (Bolivian Real): {currency_dict['BOB']}")
    print(f"Venezuela (Venezualan Bolivar): {currency_dict['VES']}")
    print(f"Paraguay (Paragyan Guarani): {currency_dict['PYG']}")
    print(f"USA (Dollars): {currency_dict['USD']}")
    
def usar_valor(**context):
    # Obtener el resultado de la variable XCOM
    value_PEN = context['ti'].xcom_pull(key='result_PEN')
    value_COP = context['ti'].xcom_pull(key='result_COP')
    value_UYU = context['ti'].xcom_pull(key='result_UYU')
    value_BRL = context['ti'].xcom_pull(key='result_BRL')
    value_ARS = context['ti'].xcom_pull(key='result_ARS')
    value_CLP = context['ti'].xcom_pull(key='result_CLP')
    value_BOB = context['ti'].xcom_pull(key='result_BOB')
    value_VES = context['ti'].xcom_pull(key='result_VES')
    value_PYG = context['ti'].xcom_pull(key='result_PYG')
    value_USD = context['ti'].xcom_pull(key='result_USD')
    print(f"Peru final rate: {float(value_PEN)*0.1}")
    print(f"Colombia final rate: {float(value_COP)*0.12}")
    print(f"Uruguay final rate: {value_UYU*0.14}")
    print(f"Brazil final rate: {value_BRL*0.09}")
    print(f"Argentina final rate: {value_ARS*0.19}")
    print(f"Chile final rate: {value_CLP*0.06}")
    print(f"Bolivia final rate: {value_BOB*0.07}")
    print(f"Venezuela final rate: {value_VES*0.34}")
    print(f"Paraguay final rate: {value_PYG*0.05}")
    print(f"USA final rate: {value_USD*0.02}")
    
with DAG('xcom_tasas_cambio_D', default_args=default_args, schedule_interval=None) as dag:
    calcular_tarea = PythonOperator(
        task_id='calcular_de_API',
        python_callable=calcular_valor,
        provide_context=True
    )
    
    usar_tarea = PythonOperator(
        task_id='usar_valor',
        python_callable=usar_valor,
        provide_context=True
    )
    
    calcular_tarea >> usar_tarea