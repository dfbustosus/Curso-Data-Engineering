from datetime import datetime
from datetime import date
from airflow import DAG
import requests
import pandas as pd 
from airflow.operators.python_operator import PythonOperator
import psycopg2
import psycopg2.extras as extras
import yfinance as yf

import smtplib

def send_mail():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('yourmail@mail.com','YourPassword1234') # your email from where you are going to send and the password that you generated with two step verification gmail
        subject='Ariflow Completed a DAG'
        body_text=f'The DAG "Get_sentiment_analysis" has been completed successfully at: { datetime.now().strftime("%H:%M:%S")} of {datetime.today().strftime("%B %d, %Y")}'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(from_addr='yourmail@mail.com',to_addrs=['Mail_to_send1@mail.com','Mail_to_send2@mail.com','Mail_to_send3@mail.com'],msg=message) # your mail from where you send , the mails or mail thta you want to send a messege and the content of the messege.
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')



def fetch_Reddit_Analysis():
    url = "https://tradestie.com/api/v1/apps/ttm-squeeze-stocks?date=2022-11-18"
    req = requests.get(url)
    redditsentiment= redditsentiment = pd.DataFrame(req.json())
    redditsentiment=redditsentiment[((redditsentiment.in_squeeze == True) & (redditsentiment.no_of_days_in_squeeze >= 2))].sort_values(by='no_of_days_in_squeeze',  ascending=False).iloc[:50,1:]
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
    
    redditsentiment= redditsentiment.dropna()


    print('Stock prices completed')
    print(f'{redditsentiment.isna().sum()}')
    print(f'Datafame dimentions{redditsentiment.shape}')

    return redditsentiment

# Function to save stock prices to Amazon Redshift
def save_to_redshift():
    stock_prices = fetch_stock_prices()

    conn = psycopg2.connect(
        host='*****',## Enter your data base host link
        port='***',
        dbname='****',## Enter the data base name
        user='*****',#Enter your user name
        password='******'#Enter your password
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
    tname='Reddit_squeeze_stocks_airflow'

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
    start_date=datetime.now(),
    tags=['Market stock','Reddit','Squeeze Analysis'],
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

Inform_task = PythonOperator( # Create a final task to inform the process has been completed 
    task_id='send_mail',
    python_callable=send_mail,
    dag=dag
)

# Define el orden de los tasks
fetch_task >> save_task >> Inform_task