from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def send():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('davidbustosusta@gmail.com','xxxxxxxxxxxxxxxxxxx')
        subject='Testing'
        body_text='Testing sucess'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('davidbustosusta@gmail.com','aileddelacruzpaez@gmail.com',message)
        print('Sucess')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2023,5,13)
}

with DAG(
    dag_id='PythonEmailDag',
    default_args=default_args,
    schedule_interval='@daily') as dag:

    start_dag=PythonOperator(
        task_id='start_dag',
        python_callable=send
    )

    start_dag