import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import requests
from datetime import timedelta

# Jinja templates: https://jinja.palletsprojects.com/en/3.1.x/
###### FASE DAG #########################

dag_path = os.getcwd()     #path original.. home en Docker


dag = DAG(
    dag_id="06_templated_query",
    schedule_interval=dt.timedelta(days=2),# dt.timedelta(days=3)
    start_date=dt.datetime(year=2023, month=6, day=19),
    end_date=dt.datetime(year=2023, month=6, day=24),
    catchup=False
)

def fetch_events(execution_date, next_execution_date, dag_path):
    start_date = execution_date.strftime('%Y-%m-%d')
    end_date = (next_execution_date + timedelta(days=1)).strftime('%Y-%m-%d')
    url = f"http://events_api:80/events?start_date={start_date}&end_date={end_date}"
    response = requests.get(url)
    with open(f"{dag_path}/data/events.json", "w") as f:
        f.write(response.text)

fetch_events_operator = PythonOperator(
    task_id="fetch_events",
    provide_context=True,
    python_callable=fetch_events,
    op_kwargs={'dag_path': dag_path},
    dag=dag
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

fetch_events_operator >> calculate_stats