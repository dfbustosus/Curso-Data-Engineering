from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd
from pandas import DataFrame
import requests

default_args={
    'owner': 'ecentoz',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

def get_input():
    url='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port='5439'
    data_base='data-engineer-database'
    user='ecentoz_coderhouse'
    pwd='46jkoB47k18P'
    rschema = 'ecentoz_coderhouse'
    table_name='valores_consulta'
    conn = psycopg2.connect(
        host=url,
        port=port,
        user=user,
        password=pwd,
        database=data_base
    )
    # Ejecuta tus consultas en Amazon Redshift
    query = '''
    SELECT time_start,time_end,activo1,activo2 from ecentoz_coderhouse.parametros_tabla_1
    '''
    cursor = conn.cursor()
    cursor.execute(query)
    input_df = DataFrame(cursor.fetchall())
    input_df.columns = ['time_start','time_end','activo1','activo2']

    # Cierra la conexión al servidor de Amazon Redshift
    cursor.close()
    conn.close()
    return input_df


def data_from_api():
    api_key = '2D8CEA2B-269D-4D95-BF1A-E9B66948F205'
    url_base = 'https://rest.coinapi.io/v1/exchangerate/'
    headers = {'X-CoinAPI-Key' : api_key}
    input = get_input()
    df_final = pd.DataFrame(columns=['time_period_start', 'time_period_end', 'time_open', 'time_close','rate_open', 'rate_high', 'rate_low', 'rate_close','activo1','activo2','par_activos'])
    for i in range(len(input)):
        activo1 = input['activo1'][i]
        activo2 = input['activo2'][i]
        time_start = input['time_start'][i].strftime('%Y-%m-%dT00:00:00')
        time_end = input['time_end'][i].strftime('%Y-%m-%dT00:00:00')
        url = url_base + activo1 + '/' + activo2 + '/history?period_id=30MIN&time_start=' + time_start + '&time_end=' + time_end + '&limit=' + '10000'
        response = requests.get(url, headers=headers)
        try:
            df = pd.DataFrame.from_dict(response.json())
            df['activo1'] = activo1
            df['activo2'] = activo2
            df['par_activos'] = activo1 + "_" + activo2
            df_final = pd.concat([df_final,df])
        except:
            df= print('Se acabó la cuota de datos, revise el último df_final para decidir como continuar')
    return df_final


def data_transform_df():
    #Genero el dataframe a completar
    df_final = data_from_api()
    columnas_fecha = ['time_period_start', 'time_period_end', 'time_open', 'time_close']
    def datetime_convert(valor):
        d = str(valor)[0:10] + ' ' + str(valor)[11:19]
        return d
    for j in range(0,len(columnas_fecha)):
        df_final[columnas_fecha[j]] = df_final[columnas_fecha[j]].apply(lambda x:datetime_convert(x))
        df_final = df_final.astype({columnas_fecha[j]: 'string'}, errors='raise')
    df_final['log_date'] = pd.to_datetime("today").strftime("%Y/%m/%d")
    return df_final

def db_connect():
    df = data_transform_df()
    url='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
    port='5439'
    data_base='data-engineer-database'
    user='ecentoz_coderhouse'
    pwd='46jkoB47k18P'
    rschema = 'ecentoz_coderhouse'
    table_name='precios_activos'
    conn = psycopg2.connect(
        host=url,
        port=port,
        user=user,
        password=pwd,
        database=data_base
    )

    cur = conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS ecentoz_coderhouse.precios_activos (
    time_period_start TIMESTAMP, time_period_end TIMESTAMP, 
    time_open TEXT, time_close TEXT,rate_open FLOAT, 
    rate_high FLOAT, rate_low FLOAT, rate_close FLOAT, 
    activo1 TEXT,activo2 TEXT, par_activos TEXT,log_date DATE
        );
    """
    cur.execute(create_table_query)
    columns = ', '.join(df.columns)
    values = ', '.join(['%s'] * len(df.columns))
    insert_query = f"""
    INSERT INTO {rschema}.{table_name} ({columns})
    VALUES ({values})
    """
    data = [tuple(row) for row in df.values]
    cur.executemany(insert_query, data)
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    default_args=default_args,
    dag_id='tercera_entrega_ecentoz',
    description= 'Actualizando información de cotización de pares de Crypto en una tabla en Redshift',
    start_date=datetime(2023,1,1,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='db_connect',
        python_callable= db_connect,
    )

    
    task1

