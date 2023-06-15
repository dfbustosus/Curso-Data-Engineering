from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args={
    'owner': 'DavidBU',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_con_conexion_postgres',
    description= 'Nuestro primer dag usando python Operator',
    start_date=datetime(2023,6,12),
    schedule_interval='0 0 * * *'
    ) as dag:
    task1= PostgresOperator(
        task_id='crear_tabla_postgres',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            create table if not exists fin_mundo(
                dt date,
                pais varchar(30)
            )
        """
    )
    task2 =PostgresOperator(
        task_id='insertar_en_tabla',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            insert into fin_mundo (dt,pais) values ('12-12-2025','Colombia');
            insert into fin_mundo (dt,pais) values ('15-08-2035','Brasil');
            insert into fin_mundo (dt,pais) values ('21-09-2030','Argentina');
            insert into fin_mundo (dt,pais) values ('13-07-2045','Chile');
            insert into fin_mundo (dt,pais) values ('17-11-2028','Ecuador');
            insert into fin_mundo (dt,pais) values ('19-03-2032','Peru');
            insert into fin_mundo (dt,pais) values ('18-08-2026','Uruguay');
            insert into fin_mundo (dt,pais) values ('22-05-2037','Paraguay');
            insert into fin_mundo (dt,pais) values ('12-12-2080','Venezuela');
            insert into fin_mundo (dt,pais) values ('12-12-2071','Mexico');
        """
    )
    task1 >> task2