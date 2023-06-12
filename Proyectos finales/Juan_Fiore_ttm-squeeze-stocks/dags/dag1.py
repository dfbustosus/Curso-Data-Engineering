import datetime
from airflow import DAG
import requests
import pandas as pd 
from airflow.operators.python_operator import PythonOperator
import psycopg2
import psycopg2.extras as extras
import yfinance as yf

def fetch_Reddit_Analysis():
    req = requests.get("https://tradestie.com/api/v1/apps/reddit")
    redditsentiment = pd.DataFrame(req.json())
    print('Sentiment Analysis Fetched') 
    print(f'Datafame dimentions{redditsentiment.shape}')
    
    return redditsentiment

# Function to fetch stock prices from Yahoo Finance API
def fetch_stock_prices():

    redditsentiment= fetch_Reddit_Analysis()

    redditsentiment['priceUSD']=0
    redditsentiment['volumeUSD']=0
    for t in redditsentiment['ticker']:
        try:
            redditsentiment.loc[redditsentiment['ticker']==t,'priceUSD'] = yf.Ticker(t).fast_info.last_price
            redditsentiment.loc[redditsentiment['ticker']==t,'volumeUSD'] = yf.Ticker(t).fast_info.last_volume
    
        except Exception as e: 
            redditsentiment.loc[redditsentiment['ticker']==t,'priceUSD']=0
    
    redditsentiment['commentScore'] =redditsentiment['sentiment_score']/ redditsentiment['no_of_comments']
    redditsentiment['nshares']=redditsentiment['volumeUSD']/redditsentiment['priceUSD']

    #El indicador de riesgo se basa en el numero de comentarios y el volumen de dinero. Suponiendo que a mayor volumen de dinero 
    #y comentarios el accet estara en masnos de mayor cantidad de genete por lo tanto mas estable es.
    #Si una accion tiene alto numero de comentarios positivo, el parametro commentScore va a ser alto y por lo tanto 
    #Acaparara mas valor del volumen dando un resultado mayor por lo tanto el indicador de riesgo estara mas cerca de 0
    # de lo contrario se ira a valores negativos o positivos dependiendo del sentimiento si es positivo o negativo
    redditsentiment['risk'] = (redditsentiment['commentScore']*redditsentiment['volumeUSD']*100)/redditsentiment['volumeUSD']
    redditsentiment= redditsentiment.dropna()


    print('Stock prices completed')
    print(f'{redditsentiment.isna().sum()}')
    print(f'Datafame dimentions{redditsentiment.shape}')

    return redditsentiment

# Function to save stock prices to Amazon Redshift
def save_to_redshift():
    stock_prices = fetch_stock_prices()

    conn = psycopg2.connect(
        host='*********',
        port='****',
        dbname='****',
        user='********',
        password='*******'
    )
    cur = conn.cursor()
    type_map = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL',
        'complex128': 'VARCHAR(50)',
        'object': 'VARCHAR(50)',
        'category': 'VARCHAR(50)',
        }  
    coldict=dict(zip(stock_prices.dtypes.index, stock_prices.dtypes.values)) 
    #Nombre de la tabla
    tname='Reddit_sentiment_market_stock_airflow'

    #generacion de las columnas para sql
    col_sql=[f'{k} {type_map[str(v)]}' for k, v in coldict.items()]

    #generacion de la table schema 
    table_schema = f"""
            CREATE TABLE IF NOT EXISTS {tname} (
                {', '.join(col_sql)}
            );
            """
    
    cur.execute(table_schema)

    
    values = [tuple(x) for x in stock_prices.to_numpy()]

    insert_sql = f"INSERT INTO {tname} ({', '.join(coldict.keys())}) VALUES %s"

    #Ejecucion de la transaccion 
   
    extras.execute_values(cur, insert_sql, values)
    conn.commit()

    print('Proceso terminado')

    cur.close()
    conn.close()

# Definimos los dags
dag = DAG(
    dag_id='stock_price_dag',
    start_date=datetime.datetime.now(),
    tags=['Market stock','Reddit','Sentiment Analysis'],
    schedule_interval='@daily',  # corre diariamente
    catchup=False
)

# Definimos los tasks
fetch_task = PythonOperator(
    task_id='fetch_stock_prices',
    python_callable=fetch_stock_prices,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_to_redshift',
    python_callable=save_to_redshift,
    dag=dag
)

# Define el orden de los tasks
fetch_task >> save_task