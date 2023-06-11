import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

###### FASE DAG #########################

dag_path = os.getcwd()     #path original.. home en Docker

dag = DAG(
    dag_id="02_daily_scheduled",
    start_date=dt.datetime(2023, 6, 11),
    end_date=dt.datetime(2023, 6, 15),
    #start_date=dt.datetime(year=2019, month=1, day=1),
    #end_date=dt.datetime(year=2019, month=1, day=5),
    schedule_interval="@daily",
)


fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        #"mkdir -p {}/data &&".format(dag_path),
        "curl -o {}/data/events.json -L 'https://raw.githubusercontent.com/dfbustosus/Curso_DS_para_todos/main/events.json'".format(dag_path)
        ),
        dag=dag,
    )

def _calculate_stats(input_path, output_path):
    """Calcular estadisticos."""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
        task_id="calcular_stats",
        python_callable=_calculate_stats,
        op_kwargs={
            "input_path": "{}/data/events.json".format(dag_path),
            "output_path": "{}/data/stats.csv".format(dag_path),
            },
        dag=dag,
    )

fetch_events >> calculate_stats