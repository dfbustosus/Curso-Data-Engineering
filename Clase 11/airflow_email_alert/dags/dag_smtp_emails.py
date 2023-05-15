from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

import smtplib

def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('davidbustosusta@gmail.com','aktmoeuyblqjuryn')
        subject='Ganaste un premio'
        body_text='Has ganado un premio fantastico!!!!'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('davidbustosusta@gmail.com','aileddelacruzpaez@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2023,5,13)
}

with DAG(
    dag_id='dag_smtp_email_automatico',
    default_args=default_args,
    schedule_interval='@daily') as dag:

    tarea_1=PythonOperator(
        task_id='dag_envio',
        python_callable=enviar
    )

    tarea_1
