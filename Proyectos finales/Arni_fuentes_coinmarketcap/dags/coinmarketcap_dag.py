from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from extract import extract_data
from transform_and_load import validate_and_load_to_redshift
from smtp_email import send_email


def run_etl_and_send_smtp_email():
    df = extract_data()
    validate_and_load_to_redshift(df_parameter=df)
    send_email()


default_args = {
    'owner': 'Arni',
    'start_date': datetime(2023, 6, 11)
}

dag = DAG(
    dag_id='dag_etl_and_email_final_',
    default_args=default_args,
    description='Extrae datos de la API coinmarketcap, los carga a Redshift y envía un correo de aviso',
    schedule_interval='@daily',
    catchup=False
)

etl_and_email = PythonOperator(
    task_id='extract_validate_load_and_send_email',
    python_callable=run_etl_and_send_smtp_email,
    dag=dag
)

