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
    dag_id="05_query_fetching",
    schedule_interval=dt.timedelta(days=3),
    start_date=dt.datetime(year=2023, month=6, day=22),
    end_date=dt.datetime(year=2023, month=6, day=25),
)


fetch_events = BashOperator(
    task_id="fetch_events",
    #bash_command=(
        #"mkdir -p {}/data &&".format(dag_path),
        #"curl -o {}/data/events.json -L 'https://raw.githubusercontent.com/dfbustosus/Curso_DS_para_todos/main/events.json'".format(dag_path)
    #    ),
    bash_command=(
        #"mkdir -p /data/events && "
        "curl -o {}/data/events.json http://events_api:80/events?start_date=2023-05-11&end_date=2023-05-14".format(dag_path)
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